#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail

# Function that will preallocate additional space for store files, will just copy anything else that will not be recognised as a store file
# param $1 source file
# param $2 target file
# param $3 file size percentage how much more space to allocate
preallocate_file() {
    declare -A STOREFILES
    STOREFILES["./neostore.labelscanstore.db"]=1
    STOREFILES["./neostore.labeltokenstore.db"]=1
    STOREFILES["./neostore.labeltokenstore.db.names"]=1
    STOREFILES["./neostore.nodestore.db"]=1
    STOREFILES["./neostore.nodestore.db.labels"]=1
    STOREFILES["./neostore.propertystore.db"]=1
    STOREFILES["./neostore.propertystore.db.arrays"]=1
    STOREFILES["./neostore.propertystore.db.index"]=1
    STOREFILES["./neostore.propertystore.db.index.keys"]=1
    STOREFILES["./neostore.propertystore.db.strings"]=1
    STOREFILES["./neostore.relationshipstore.db"]=1
    STOREFILES["./neostore.relationshipgroupstore.db"]=1
    STOREFILES["./neostore.relationshiptypestore.db"]=1
    STOREFILES["./neostore.relationshiptypestore.db.names"]=1
    STOREFILES["./neostore.schemastore.db"]=1

   tput setaf 2
   if [ "${STOREFILES["${1}"]}" = "1" ]
    then
        CURRENT_SIZE=$(stat -c%s "$1")
        ADDITIONAL_SIZE=$(awk -v c=$CURRENT_SIZE -v m=${3} 'BEGIN { print int(((c * m) / 100 )) }')
        NEW_SIZE=$((CURRENT_SIZE + ADDITIONAL_SIZE))
        dd if=/dev/zero of="${2}" bs=1024 count=$(((NEW_SIZE+512)/1024)) status=none
   fi
   dd if="${1}" of="${2}" bs=8M status=none conv=notrunc oflag=direct
   tput sgr0
}

export -f preallocate_file

if [ $# -lt 2 ]
    then
        tput setaf 1;
        echo "FAIL: Copy store tool usage example: ./storecopy.sh source_database_directory target_database_directory [additional preallocation percentage].";
        tput sgr0;
        exit 1
fi

SOURCE="$1"
TARGET="$2"
ADDITIONAL_PREALLOCATION_PERCENTAGE="10"
if [ $# -eq 3 ]
  then
  ADDITIONAL_PREALLOCATION_PERCENTAGE="$3"
fi


echo "Source directory: ${SOURCE}"
echo "Target directory: ${TARGET}"
echo "Preallocation space percentage: ${ADDITIONAL_PREALLOCATION_PERCENTAGE}"

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
find . -type f -exec bash -c 'preallocate_file "$0" "$1" "$2" $3' {} "$TARGET/{}" "$ADDITIONAL_PREALLOCATION_PERCENTAGE" \;
echo "*****New store at {$TARGET} created.*****"
