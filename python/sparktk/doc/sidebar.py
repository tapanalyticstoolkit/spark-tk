# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


"""
Small module which hacks at HTML to add links to class properties to the "sidebar"

usage:  sidebar.py <file.html>
"""

import re
import tempfile
import os
import sys
import shutil


def collect_class_properties(path):
    """
    Returns a list of tuples (name, link-ID) of class entries from the "Instance varaibles" heading for a given file

    NB: will only collect the ones for the first class in the file
    """
    INIT = 0
    START = 1
    state = INIT

    expr = re.compile('\s*<p id=\"(.*?)\" class="name">var <span class=\"ident\">(\w+)</span></p>\s*')

    pairs = {}
    with open(path, "r") as reader:
        for line in reader.readlines():
            if state == INIT:
                if line.strip().startswith('<h3>Instance variables'):
                    state = START
            elif state == START:
                if line.strip().startswith('<p id="sparktk'):
                    match = expr.match(line)
                    if match:
                        id, name = match.groups()
                        pairs[name] = id

                elif '<h3>Methods</h3>' in line:
                    break
    return sorted(pairs.items())

def add_to_sidebar(path, name_link_pairs):
    """
    Rewrites the given file as it adds the name-link pairs to the sidebar
    """
    INIT = 0
    START = 1
    DONE = 2
    state = INIT

    if not name_link_pairs:
        return

    p = '\s*<li class=\"mono\"><a href=\".*\">(\w+)</a></li>'
    expr = re.compile(p)

    def get_entry(pair):
        n, i = pair
        return  '<li class="mono"><a href="#%s">%s</a></li>\n' % (i, n)

    with open(path, 'r') as r:
        with tempfile.NamedTemporaryFile(delete=False) as w:
            tmp_name = w.name
            for line in r.readlines():
                if state == INIT:
                    x = line.strip()
                    if x.startswith('<li') and x.endswith('__init__</a></li>'):
                        state = START
                elif state == START:
                    p = '\s*<li class=\"mono\"><a href=\".*\">(\w+)</a></li>'
                    match = expr.match(line)
                    if match:
                        name, = match.groups()
                        if name_link_pairs[0][0] < name:
                            w.write(get_entry(name_link_pairs[0]))
                            name_link_pairs.pop(0)
                            if not name_link_pairs:
                                state = DONE
                    else:
                        for t in name_link_pairs:
                            w.write(t)
                        state = DONE

                w.write(line)

    os.remove(path)
    shutil.move(tmp_name, path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise RuntimeError("missing file path argument")
    path = sys.argv[1]
    prop_links = collect_class_properties(path)
    add_to_sidebar(path, prop_links)
