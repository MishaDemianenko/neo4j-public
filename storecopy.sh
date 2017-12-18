#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail

if [ $# -lt 2 ]
    then
        tput setaf 1;
        echo "FAIL: Copy store tool usage example: ./storecopy.sh source_database_directory target_database_directory.";
        tput sgr0;
        exit 1
fi

SOURCE="$1"
TARGET="$2"

echo "Source directory: ${SOURCE}"
echo "Target directory: ${TARGET}"

if [ ! -f "${SOURCE}/neostore" ]
    then
        tput setaf 1;
        echo "FAIL: source directory should be neo4j database directory."
        tput sgr0;
        exit 1
fi

if [ -d "${TARGET}" ]
   then
       tput setaf 1;
       echo "FAIL: target directory already exist. Please specify non existent directory."
       tput sgr0;
       exit 1
fi

echo "*****Start store copying*****"
echo "Creating ${TARGET} directory..."
mkdir -p "${TARGET}"

cd "${SOURCE}"
find . -mindepth 1 -type d -printf '\033[0;32mCreating directory %p\n\033[0m' -exec mkdir -p "$TARGET/{}" \;
find . -type f -printf '\033[0;32mCopying file %p\n\033[0m' -exec dd if={} of="$TARGET/{}" bs=8M status=progress oflag=direct \;

echo "*****Store copy at ${TARGET} created*****"
