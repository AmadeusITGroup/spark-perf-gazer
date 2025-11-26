#!/bin/bash

set -e

usage() {
  echo "Usage: $0 <version>"
  echo ""
  echo "Release a new version by creating and pushing a git tag."
  echo ""
  echo "Arguments:"
  echo "  version    Version number in format X.Y.Z or X.Y.Z-QUALIFIER"
  echo "             (e.g., 1.2.3, 1.2.3-SNAPSHOT, 1.2.3-RC1)"
  echo ""
  echo "Examples:"
  echo "  $0 1.2.3"
  echo "  $0 1.2.3-SNAPSHOT"
  echo "  $0 1.2.3-RC1"
  exit 1
}

# Check if version parameter is provided
if [ "$#" -ne 1 ]; then
  echo "Error: Version parameter is required."
  echo ""
  usage
fi

version="$1"

# Validate version format (X.Y.Z or X.Y.Z-QUALIFIER)
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9]+)?$ ]]; then
  echo "Error: Invalid version format '$version'"
  echo "Version must be in format X.Y.Z or X.Y.Z-QUALIFIER (e.g., 1.2.3, 1.2.3-SNAPSHOT)"
  echo ""
  usage
fi

echo "Creating release for version: $version"

# Create annotated tag
git tag -a "v$version" -m "v$version"
echo "Tag v$version created successfully"

# Push tag to origin
git push origin "v$version"
echo "Tag v$version pushed to origin"
