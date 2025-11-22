"""
LinkedIn profile processing utilities.

Functions for processing raw Apify profile data and matching to guests.
"""

import json


def normalize_linkedin_handle(linkedin_url_or_handle):
    """
    Normalize LinkedIn handle to /in/<handle> format for comparison.

    Examples:
        "https://linkedin.com/in/artsofbaniya/" → "/in/artsofbaniya"
        "/in/ArtsOfBaniya" → "/in/artsofbaniya"
        "artsofbaniya" → "/in/artsofbaniya"

    Args:
        linkedin_url_or_handle: LinkedIn URL, /in/ path, or just the handle

    Returns:
        str: Normalized format /in/<handle> in lowercase
    """
    # Remove URL prefixes if present
    handle = linkedin_url_or_handle.replace('https://www.linkedin.com', '')
    handle = handle.replace('https://linkedin.com', '')
    handle = handle.replace('http://www.linkedin.com', '')
    handle = handle.strip('/')

    # Ensure /in/ prefix
    if not handle.startswith('in/'):
        handle = f'in/{handle}'

    # Add leading slash and lowercase
    return f'/{handle}'.lower()


def create_lookup_from_apify_profiles(apify_profiles):
    handle_to_profile = {}
    for profile in apify_profiles:
        public_id = profile.get('publicIdentifier', '')
        if public_id:
            normalized_handle = normalize_linkedin_handle(public_id)
            handle_to_profile[normalized_handle] = profile
    return handle_to_profile


def match_record_to_apify_profile(record, apify_profile_lookup):
    linkedin_handle = record[3]
    normalized_handle = normalize_linkedin_handle(linkedin_handle)
    return apify_profile_lookup.get(normalized_handle)


def partition_records_by_profile_availability(pending_records, apify_profile_lookup):
    with_profiles = []
    without_profiles = []

    for record in pending_records:
        apify_profile = match_record_to_apify_profile(record, apify_profile_lookup)
        if apify_profile:
            with_profiles.append((apify_profile, record))
        else:
            without_profiles.append(record)

    return with_profiles, without_profiles


def build_enriched_profile_record(apify_profile, record):
    github_user_id = record[0]
    social_link_url = record[1]
    link_provider = record[2]
    linkedin_handle = record[3]

    return (
        github_user_id,
        social_link_url,
        link_provider,
        linkedin_handle,
        'apify',  # record_source
        True,     # profile_found
        None,     # profile_fetch_message
        apify_profile.get('fullName'),
        apify_profile.get('firstName'),
        apify_profile.get('lastName'),
        apify_profile.get('headline'),
        apify_profile.get('about'),
        apify_profile.get('publicIdentifier'),
        apify_profile.get('linkedinUrl'),
        apify_profile.get('connections'),
        apify_profile.get('followers'),
        apify_profile.get('jobTitle'),
        apify_profile.get('companyName'),
        apify_profile.get('companyIndustry'),
        apify_profile.get('companyWebsite'),
        apify_profile.get('companyLinkedin'),
        apify_profile.get('companyFoundedIn'),
        apify_profile.get('companySize'),
        apify_profile.get('currentJobDurationInYrs'),
        apify_profile.get('addressWithCountry'),
        apify_profile.get('addressCountryOnly'),
        apify_profile.get('addressWithoutCountry'),
        apify_profile.get('profilePic'),
        apify_profile.get('profilePicHighQuality'),
        apify_profile.get('topSkillsByEndorsements'),
        json.dumps(apify_profile),  # profile_data JSONB
        0,     # retry_count (reset to 0 on success)
        None,  # last_retry_at (clear on success)
        None,  # next_retry_after (clear on success)
    )


def build_missing_profile_record(record, current_retry_count=0):
    github_user_id = record[0]
    social_link_url = record[1]
    link_provider = record[2]
    linkedin_handle = record[3]

    import time
    current_timestamp = int(time.time() * 1000)  # epoch milliseconds
    new_retry_count = current_retry_count + 1

    # Exponential backoff: 5min, 20min, 45min (retry_count^2 * 5 minutes)
    backoff_minutes = (new_retry_count ** 2) * 5
    next_retry_timestamp = current_timestamp + (backoff_minutes * 60 * 1000)

    return (
        github_user_id,
        social_link_url,
        link_provider,
        linkedin_handle,
        'apify',  # record_source
        False,    # profile_foundgi
        'Profile not returned by Apify API',  # profile_fetch_message
        *([None] * 23),  # 23 None values for profile fields (full_name through top_skills_by_endorsements)
        None,  # profile_data JSONB
        new_retry_count,  # retry_count (incremented)
        current_timestamp,  # last_retry_at
        next_retry_timestamp,  # next_retry_after (exponential backoff)
    )
