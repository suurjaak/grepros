#!/usr/bin/env python3
"""Prints standard input with triple-backticked blocks nested in <pre>-tags."""
import re, sys

# Input lines from doxypypy are like:
#    #        @param   args.write_options   ```
#    #                                      {"column": additional columns,
print(re.sub("\n(\s*#)?([^\n]+)```(.+)```",  # 1: leading whitespace + #, 2: @param-decl, 3: content
             lambda m: "\n%s%s<pre>%s</pre>" % (m[1], m[2], "\n".join(
                 # Retain leading whitespace + #, strip interleaving whitespace from content lines
                 x[:len(m[1])] + re.sub("^ {%s}" % len(m[2]), "", x[len(m[1]):])
                 for x in m[3].splitlines()
             )), sys.stdin.read(), flags=re.DOTALL))
