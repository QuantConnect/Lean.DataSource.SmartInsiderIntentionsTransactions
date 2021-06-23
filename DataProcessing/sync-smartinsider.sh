#!/bin/bash

FTP_HOST="sftp://${FTP_HOSTNAME}"
FTP_BACKUP_HOST="sftp://${FTP_BACKUP_HOSTNAME}"
FTP_OPTIONS="--delete --only-missing --parallel=4"

INTENTIONS_OUTPUT_DIR="/raw/alternative/smartinsider/intentions"
TRANSACTIONS_OUTPUT_DIR="/raw/alternative/smartinsider/transactions"

mkdir -p $HOME/.ssh/
touch $HOME/.ssh/known_hosts

ssh-keyscan -H ${FTP_HOSTNAME} >> $HOME/.ssh/known_hosts
ssh-keyscan -H ${FTP_BACKUP_HOSTNAME} >> $HOME/.ssh/known_hosts

function get_file {
    local url=$1
    local backup_url=$2
    local output_dir=$3
    local username=$4
    local password=$5
    local name=$6

    local temp_output_dir="$(mktemp -d)/${name}"

    mkdir -p "$temp_output_dir"
    mkdir -p "$output_dir"

    echo "Begin downloading ${name} data from server: ${url}"
    lftp -u "${username}":"${password}" "$url" <<EOF
mirror $FTP_OPTIONS --target-directory "$temp_output_dir" -v
exit
EOF

    if [ $? -ne 0 ]
    then
        echo "Failed to get ${name} data from main server, using backup server: ${backup_url}"
        lftp -u "${username}":"${password}" "$backup_url" <<EOF
mirror $FTP_OPTIONS --target-directory "$temp_output_dir" -v
exit
EOF
        if [ $? -ne 0 ]
        then
            echo "Failed to get ${name} data from backup server. Returning with code 1"
            return 1
        fi
    fi

    for i in $(ls "$temp_output_dir"); do
        echo "Moving file from ${temp_output_dir}/${i} to ${output_dir}/${i}"
        mv -n "${temp_output_dir}"/"${i}" "${output_dir}"/"${i}"
    done
    
    rm -rf "$temp_output_dir"

    return 0
}

get_file $FTP_HOST $FTP_BACKUP_HOST $INTENTIONS_OUTPUT_DIR $INTENTIONS_USERNAME $INTENTIONS_PASSWORD "intentions"
echo "Downloading intentions data finished with exit code $?"

get_file $FTP_HOST $FTP_BACKUP_HOST $TRANSACTIONS_OUTPUT_DIR $TRANSACTIONS_USERNAME $TRANSACTIONS_PASSWORD "transactions"
echo "Downloading transactions data finished with exit code $?"
