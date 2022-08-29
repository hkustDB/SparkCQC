#!/bin/bash

function average {
    file=$1
    count=0
    total=0
    file_path=$(dirname "${file}")
    base_name=$(basename "${file}")
    raw_name=${base_name%.txt}
    avg_file="${file_path}/${raw_name}.avg"

    for i in $( awk '{ print $1; }' ${file} )
        do
            if [[ -n $i ]]; then
                awk_result=$(echo "$i 0" | awk '{if ($1 < $2) print "0"; else print "1"}')
                if [[ awk_result -eq "1" ]]; then
                    total=$(echo $total+$i | bc )
                    ((count++))
                fi
            fi
        done

    rm -f "${avg_file}"
    touch "${avg_file}"
    if [[ $count -gt 0 ]]; then
        result=$(echo "scale=2; $total / $count" | bc)
        echo "${result}" > "${avg_file}"
    fi
}

result_path=$1
if [[ ! -d ${result_path} ]]; then
    exit 1
fi

files=$(find "${result_path}" -maxdepth 1 -type f -name 'task*.txt')
for file in ${files[@]}
do
    average ${file}
done