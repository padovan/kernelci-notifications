#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import hashlib
import json
import sys
import urllib

import requests
from datetime import datetime, timezone, timedelta

DASHBOARD_API = "https://dashboard.kernelci.org/api/"


def _hash_url(url):
    """
    Generate a unique hashed file name for a given URL.

    Args:
        url (str): The URL to hash.

    Returns:
        str: A hashed file name with .json extension.
    """
    return hashlib.sha256(url.encode('utf-8')).hexdigest() + '.json'


def json_cache_store(data, file_name):
    """
    Stores JSON data into a local cache file.

    Args:
        data (dict): The JSON data to store.
        file_name (str): The URL to use as a reference for the file.
    """
    cache_dir = "dashboard_json_cache"
    os.makedirs(cache_dir, exist_ok=True)  # Ensure the cache directory exists

    hashed_file_name = _hash_url(file_name)
    file_path = os.path.join(cache_dir, hashed_file_name)
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)


def json_cache_retrieve(file_name):
    """
    Retrieves JSON data from a local cache file.

    Args:
        file_name (str): The URL to use as a reference for the file.

    Returns:
        dict: The JSON data from the file, or None if the file does not exist.

    Raises:
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    cache_dir = "dashboard_json_cache"
    hashed_file_name = _hash_url(file_name)
    file_path = os.path.join(cache_dir, hashed_file_name)

    if not os.path.exists(file_path):
        return None

    with open(file_path, 'r') as json_file:
        return json.load(json_file)


def fetch_from_api(endpoint, params = {}, use_cache=True):
    base_url = urllib.parse.urljoin(DASHBOARD_API, endpoint)
    try:
        url = "{}?{}".format(base_url, urllib.parse.urlencode(params))
        json = None
        if use_cache:
            json = json_cache_retrieve(url)
        if json:
            return json
        r = requests.get(url)
    except:
        print(f"Failed to fetch from {DASHBOARD_API}.")
        sys.exit(1)

    try:
        json = r.json()
        json_cache_store(json, url)
        return json
    except:
        print(r.content)
        raise


def fetch_full_results(origin, giturl, branch, commit):
    endpoint = f"tree/{commit}/full"
    params = {
        "origin": origin,
        "git_url": giturl,
        "git_branch": branch,
        "commit": commit,
    }

    return fetch_from_api(endpoint, param)


def fetch_summary(origin, giturl, branch, commit, timestamp):
    commit_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    current_time = datetime.now(tz=timezone.utc)
    time_3_hours_ago = current_time - timedelta(hours=3)

    use_cache = not (commit_time > time_3_hours_ago)

    endpoint = f"tree/{commit}/summary"
    params = {
        "origin": origin,
        "git_url": giturl,
        "git_branch": branch,
        "commit": commit,
    }

    return fetch_from_api(endpoint, params, use_cache)


def fetch_commits(origin, giturl, branch, commit):
    endpoint = f"tree/{commit}/commits"
    params = {
        "origin": origin,
        "git_url": giturl,
        "git_branch": branch,
        "commit": commit,
    }

    return fetch_from_api(endpoint, params)


def fetch_test(test_id):
    endpoint = f"test/{test_id}"

    return fetch_from_api(endpoint)


def fetch_build(build_id):
    endpoint = f"build/{build_id}"

    return fetch_from_api(endpoint)


def fetch_issue_tests(issue, version):
    endpoint = f"issue/{issue}/version/{version}/tests"

    return fetch_from_api(endpoint)


def fetch_issue_builds(issue, version):
    endpoint = f"issue/{issue}/version/{version}/builds"

    return fetch_from_api(endpoint)


def fetch_issue(issue, version):
    endpoint = f"issue/{issue}/version/{version}"

    return fetch_from_api(endpoint)


def fetch_tree_fast(origin):
    params = {
        "origin": origin,
    }
    return fetch_from_api("tree-fast", params, use_cache=False)