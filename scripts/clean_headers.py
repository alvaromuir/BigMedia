#!/usr/bin/python
# This script takes a 'fields' row from a Google DFA report file
# and makes it db import friendly for all of ya big data needs
# @alvaromuir, 10/2014

import sys

if(len(sys.argv) < 2):
    print
    print "Error - Requires an input file"
    print "usage: " + sys.argv[0] + " <input_file> <optional output_file>"
    sys.exit()

def get_headers(args):
    file = open(args)
    raw = file.read().rstrip()
    headers = raw.split(',\"')
    fields = []

    def replace_inner_comma(str):
        str = str.replace(".,","_")
        str = str.replace(",","_")
        return str

    def title_friendly(str):
        str = str.replace('\n',"")
        str = str.replace("(","").replace(")","").replace("%","percent")
        str = str.replace(" - ","_").replace("-","_")
        str = str.replace(" : ","_").replace(": "," ").replace("_:"," ")
        str = str.replace("/","_")
        str = str.replace(" ","_")
        return str

    def get_lines(str):
        lines = str.split(',')
        return lines

    for el in headers:
        el = el.lower()
        if (len(el.split("\"")) == 1):
            lines = get_lines(el)
            for line in lines:
                if (len(line) > 0):
                    line = title_friendly(line)
                    fields.append(line.strip())
        else:
            needs_work = el.split("\"")
            for lines in needs_work:
                field = lines
                if (len(field) > 0):
                    if (not field.startswith(',')):
                        field = replace_inner_comma(field)
                        field = title_friendly(field)
                        fields.append(line.strip())
                    else:
                        lines = get_lines(field)
                        for line in lines:
                            if (len(line) > 0):
                                line = title_friendly(line)
                                fields.append(line.strip())

        return ','.join(map(str,fields))

if(len(sys.argv) < 3):
    print
    print len(get_headers(sys.argv[1]).rstrip().split(',')), "fields"
    print "---"
    print get_headers(sys.argv[1]).rstrip()
    sys.exit()
else:
    writer = open(sys.argv[2],"wb")
    writer.write(get_headers(sys.argv[1]).rstrip())
    writer.write('\r')
    writer.close()
    sys.exit()
