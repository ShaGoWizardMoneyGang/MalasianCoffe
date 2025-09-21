#!/bin/bash

GO_VERSION="1.24.6"

MODULES_DIFF_VERSION=$(find -name "go.mod" -exec grep go\ 1.* \{\} + | sed 's/go//g' | grep -v $GO_VERSION)
AMOUNT=$(echo ${MODULES_DIFF_VERSION} | sed '/^\s*$/d' | wc -l)

if [ ${AMOUNT} -ne 0 ]; then
    echo "ERROR: ${AMOUNT} files differ in go version. All files must have the same version: ${GO_VERSION}"
    echo "Change the following files, so that they read 'go ${GO_VERSION}'"
    while IFS= read -r line ; do
        echo $(echo $line | awk '{print $1}');
    done <<< "$MODULES_DIFF_VERSION"
    exit 1
else
    exit 0
fi
