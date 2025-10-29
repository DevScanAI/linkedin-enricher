"""
LinkedIn Profile Enrichment Script

Fetches LinkedIn profile data for guests and stores in database.
"""

import argparse
from lib.db.postgres import open_connection, close_connection, execute_query, upsert_multiple_records
from lib.apify import get_linkedin_profiles
from lib.utils import (
    create_lookup_from_apify_profiles,
    partition_guests_by_profile_availability,
    build_enriched_profile_record,
    build_missing_profile_record,
)


def fetch_guests_pending_for_enrichment(limit, luma_event_api_id=None):
    """
    Fetch guests with LinkedIn handles needing enrichment or retry.

    Fetches guests that either:
    1. Have never been enriched (no record in linkedin_profiles)
    2. Failed enrichment but under max retry limit (retry_count < 3)
    3. Retry backoff period has passed (next_retry_after < current_time)

    Args:
        limit: Maximum number of guests to fetch
        luma_event_api_id: Optional event ID to filter guests by specific event

    Returns:
        list: List of tuples (luma_guest_api_id, guest_name, linkedin_handle, retry_count)
              or None if connection failed
    """
    import time
    current_timestamp = int(time.time() * 1000)

    # Build event filter join if needed
    event_join = ""
    event_filter = ""
    params = [current_timestamp]

    if luma_event_api_id:
        event_join = "JOIN luma.event_guests eg ON eg.luma_guest_api_id = g.luma_guest_api_id"
        event_filter = "AND eg.luma_event_api_id = %s"
        params.append(luma_event_api_id)

    params.append(limit)

    query = f"""
    SELECT
        g.luma_guest_api_id,
        g.guest_name,
        g.linkedin_handle,
        COALESCE(lp.retry_count, 0) as retry_count
    FROM luma.guests g
    {event_join}
    LEFT JOIN luma.linkedin_profiles lp
        ON lp.luma_guest_api_id = g.luma_guest_api_id
    WHERE g.linkedin_handle IS NOT NULL
    {event_filter}
    AND (
        -- Never enriched
        lp.luma_guest_api_id IS NULL
        OR
        -- Failed enrichment eligible for retry
        (
            lp.profile_found = FALSE
            AND lp.retry_count < 3
            AND (lp.next_retry_after IS NULL OR lp.next_retry_after < %s)
        )
    )
    ORDER BY
        COALESCE(lp.retry_count, 0) ASC,  -- Prioritize new attempts
        lp.last_retry_at ASC NULLS FIRST  -- Then oldest retries
    LIMIT %s
    """

    if open_connection():
        results = execute_query(query, params=tuple(params), is_select_query=True)
        close_connection()
        return results
    return None


def upsert_linkedin_profiles(profile_records):
    """
    Batch upsert profile records to database.

    Args:
        profile_records: List of tuples (database records)

    Returns:
        bool: True if successful, False otherwise
    """
    if not profile_records:
        print("No records to upsert")
        return True

    # Column order must match the record tuple order in build_*_profile_record()
    columns = [
        'luma_guest_api_id',
        'linkedin_handle',
        'record_source',
        'profile_found',
        'profile_fetch_message',
        'full_name',
        'first_name',
        'last_name',
        'headline',
        'about',
        'public_identifier',
        'linkedin_url',
        'connections',
        'followers',
        'job_title',
        'company_name',
        'company_industry',
        'company_website',
        'company_linkedin',
        'company_founded_in',
        'company_size',
        'current_job_duration_yrs',
        'address_with_country',
        'address_country_only',
        'address_without_country',
        'profile_pic_url',
        'profile_pic_high_quality_url',
        'top_skills_by_endorsements',
        'profile_data',
        'retry_count',
        'last_retry_at',
        'next_retry_after',
    ]

    # Columns to detect conflicts (unique constraint)
    conflict_columns = ['luma_guest_api_id', 'linkedin_handle']

    # Columns to update on conflict (all except conflict columns)
    update_columns = [col for col in columns if col not in conflict_columns]

    if open_connection():
        try:
            upsert_multiple_records(
                profile_records,
                'luma.linkedin_profiles',
                columns,
                conflict_columns,
                update_columns
            )
            close_connection()
            return True
        except Exception as e:
            print(f"Failed to upsert records: {e}")
            close_connection()
            return False
    return False


def enrich_linkedin_profiles(batch_size, luma_event_api_id=None):
    """
    Main enrichment workflow.

    1. Fetch guests needing enrichment
    2. Call Apify API to get LinkedIn profiles
    3. Match returned profiles to guests
    4. Build database records for matches and misses
    5. Upsert all records to database

    Args:
        batch_size: Number of guests to process in this batch
        luma_event_api_id: Optional event ID to filter guests by specific event
    """
    # Step 1: Fetch guests from database
    pending_guests = fetch_guests_pending_for_enrichment(batch_size, luma_event_api_id)
    if not pending_guests:
        event_msg = f" for event {luma_event_api_id}" if luma_event_api_id else ""
        print(f"No guests need LinkedIn enrichment{event_msg}")
        return

    # Count retries vs new attempts
    retry_counts = [guest[3] if len(guest) > 3 else 0 for guest in pending_guests]
    new_attempts = sum(1 for count in retry_counts if count == 0)
    retries = len(pending_guests) - new_attempts

    event_msg = f" for event {luma_event_api_id}" if luma_event_api_id else ""
    print(f"Found {len(pending_guests)} guests needing enrichment{event_msg}")
    if retries > 0:
        print(f"  - {new_attempts} new attempts")
        print(f"  - {retries} retries")

    # Step 2: Build LinkedIn URLs and call Apify API
    linkedin_urls = [
        f"https://www.linkedin.com{guest[2]}"
        for guest in pending_guests
    ]
    apify_profiles = get_linkedin_profiles(linkedin_urls)

    # Step 3: Create lookup and partition guests
    profile_lookup = create_lookup_from_apify_profiles(apify_profiles)
    guests_with_profiles, guests_without_profiles = partition_guests_by_profile_availability(
        pending_guests,
        profile_lookup
    )

    print(f"Matched {len(guests_with_profiles)} profiles, {len(guests_without_profiles)} missing")

    # Step 4: Build database records
    enriched_records = [
        build_enriched_profile_record(profile, guest)
        for profile, guest in guests_with_profiles
    ]
    missing_records = [
        build_missing_profile_record(guest)
        for guest in guests_without_profiles
    ]

    # Step 5: Upsert to database
    all_records = enriched_records + missing_records
    success = upsert_linkedin_profiles(all_records)

    if success:
        print(f"✅ LinkedIn enrichment complete!")
        print(f"   Enriched: {len(enriched_records)}")
        print(f"   Missing: {len(missing_records)}")
    else:
        print("❌ Failed to save records to database")


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description='Enrich guest records with LinkedIn profile data'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=5,
        help='Number of profiles to enrich (default: 5)'
    )
    parser.add_argument(
        '--profiles',
        type=str,
        default=None,
        help='Comma-separated LinkedIn handles for manual enrichment'
    )
    parser.add_argument(
        '--event-id',
        type=str,
        default=None,
        help='Luma event API ID to filter guests by specific event'
    )

    args = parser.parse_args()

    if args.profiles:
        # TODO: Implement manual mode
        print(f"Manual mode not yet implemented")
        print(f"Will process: {args.profiles}")
    else:
        enrich_linkedin_profiles(args.count, args.event_id)


if __name__ == '__main__':
    main()
