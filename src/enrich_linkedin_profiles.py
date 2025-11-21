"""
LinkedIn Profile Enrichment Script

Fetches LinkedIn profile data for pending records and stores in database.
"""

import argparse
from lib.db.postgres import open_connection, close_connection, execute_query, upsert_multiple_records
from lib.apify import get_linkedin_profiles
from lib.utils import (
    create_lookup_from_apify_profiles,
    partition_records_by_profile_availability,
    build_enriched_profile_record,
    build_missing_profile_record,
)


def fetch_records_pending_for_enrichment(limit):
    import time
    current_timestamp = int(time.time() * 1000)

    params = [current_timestamp]
    params.append(limit)

    query = f"""
    SELECT
        lud.github_user_id,
        lud.social_link_url,
        lud.link_provider,
        lud.linkedin_handle,
        COALESCE(lud.retry_count, 0) as retry_count
    FROM linkedin.user_details lud
    WHERE lud.linkedin_handle IS NOT NULL
    AND (
        -- Never enriched
        lud.retry_count IS NULL
        OR
        -- Failed enrichment eligible for retry
        (
            lud.profile_found IS FALSE
            AND lud.retry_count < 3
            AND (lud.next_retry_after IS NULL OR lud.next_retry_after < %s)
        )
    )
    ORDER BY
        COALESCE(lud.retry_count, 0) ASC,  -- Prioritize new attempts
        lud.last_retry_at ASC NULLS FIRST  -- Then oldest retries
    LIMIT %s
    """

    if open_connection():
        results = execute_query(query, params=tuple(params), is_select_query=True)
        close_connection()
        return results
    return None


def upsert_linkedin_profiles(profile_records):
    if not profile_records:
        print("No records to upsert")
        return True

    # Column order must match the record tuple order in build_*_profile_record()
    columns = [
        'github_user_id',
        'social_link_url',
        'link_provider',
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
    conflict_columns = ['github_user_id', 'linkedin_handle']

    # Columns to update on conflict (all except conflict columns)
    update_columns = [col for col in columns if col not in conflict_columns]

    if open_connection():
        try:
            upsert_multiple_records(
                profile_records,
                'linkedin.user_details',
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


def enrich_linkedin_profiles(batch_size):
    # Step 1: Fetch guests from database
    pending_records = fetch_records_pending_for_enrichment(batch_size)
    if not pending_records:
        print(f"No records need LinkedIn enrichment")
        return

    # Count retries vs new attempts
    retry_counts = [record[4] for record in pending_records]
    new_attempts = sum(1 for count in retry_counts if count == 0)
    retries = len(pending_records) - new_attempts

    print(f"Found {len(pending_records)} guests needing enrichment")
    if retries > 0:
        print(f"  - {new_attempts} new attempts")
        print(f"  - {retries} retries")    

    # Step 2: Build LinkedIn URLs and call Apify API
    linkedin_urls = [
        f"https://www.linkedin.com/{record[3]}"
        for record in pending_records
    ]
    apify_profiles = get_linkedin_profiles(linkedin_urls)

    # Step 3: Create lookup and partition records
    profile_lookup = create_lookup_from_apify_profiles(apify_profiles)
    records_with_profiles, records_without_profiles = partition_records_by_profile_availability(
        pending_records,
        profile_lookup
    )

    print(f"Matched {len(records_with_profiles)} profiles, {len(records_without_profiles)} missing")

    # Step 4: Build database records
    enriched_records = [
        build_enriched_profile_record(profile, record)
        for profile, record in records_with_profiles
    ]
    missing_records = [
        build_missing_profile_record(record)
        for record in records_without_profiles
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

    args = parser.parse_args()

    if args.profiles:
        # TODO: Implement manual mode
        print(f"Manual mode not yet implemented")
        print(f"Will process: {args.profiles}")
    else:
        enrich_linkedin_profiles(args.count)


if __name__ == '__main__':
    main()
