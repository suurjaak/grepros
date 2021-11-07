"""
A light and fast template engine.

Copyright (c) 2012, Daniele Mazzocchio
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
* Neither the name of the developer nor the names of its contributors may be
  used to endorse or promote products derived from this software without
  specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

------------------------------------------------------------------------------

Supplemented with escape and collapse and newline and unbuffered options,
and caching compiled code objects,
by Erki Suurjaak.
"""

import re
import sys


PY3 = (sys.version_info > (3, 0))
text_types = (str, ) if PY3 else (str, unicode)


class Template(object):

    TRANSPILED_TEMPLATES = {} # {(template string, compile options): compilable code string}
    COMPILED_TEMPLATES   = {} # {compilable code string: code object}
    # Regex for stripping all leading, trailing and interleaving whitespace (line-based).
    RE_STRIP = re.compile("(^[ \t]+|[ \t]+$|(?<=[ \t])[ \t]+|\A[\r\n]+|[ \t\r\n]+\Z)", re.M)

    def __init__(self, template, strip=True, escape=False, collapse=False):
        """Initialize class"""
        super(Template, self).__init__()
        self.template = template
        self.options  = {"strip": strip, "escape": escape, "collapse": collapse}
        self.builtins = {"escape": escape_html,
                         "setopt": lambda k, v: self.options.update({k: v}), }
        cache_key = (template, bool(escape))
        if cache_key in Template.TRANSPILED_TEMPLATES:
            source = Template.TRANSPILED_TEMPLATES[cache_key]
        else:
            source = self._process(self._preprocess(self.template))
            Template.TRANSPILED_TEMPLATES[cache_key] = source
        if source in Template.COMPILED_TEMPLATES:
            self.code = Template.COMPILED_TEMPLATES[source]
        else:
            self.code = compile(source, "<string>", "exec")
            Template.COMPILED_TEMPLATES[source] = self.code

    def expand(self, namespace={}, **kw):
        """Return the expanded template string"""
        output = []
        namespace.update(kw, **self.builtins)
        namespace["echo"]  = lambda s: output.append(s)
        namespace["isdef"] = lambda v: v in namespace

        eval(self.code, namespace)
        return self._postprocess("".join(map(to_unicode, output)))

    def stream(self, buffer, namespace={}, encoding="utf-8", newline="\r\n",
               unbuffered=False, **kw):
        """
        Expand the template and stream it to a file-like buffer.

        @param   newline     if specified, converts \r \n \r\n linefeeds to this
        @param   unbuffered  whether stream is written immediately
        """

        def write_buffer(s, flush=False, cache=[""]):
            # Cache output as a single string and write to buffer.
            s = to_unicode(s)
            if newline is not None:
                s = re.sub("(\r(?!\n))|((?<!\r)\n)|(\r\n)", newline, s)
            cache[0] += s
            if (flush or unbuffered) and cache[0] or len(cache[0]) > 65536:
                buffer.write(self._postprocess(cache[0]).encode(encoding))
                cache[0] = ""

        namespace.update(kw, **self.builtins)
        namespace["echo"]  = write_buffer
        namespace["isdef"] = lambda v: v in namespace

        eval(self.code, namespace)
        write_buffer("", flush=True) # Flush any last cached bytes

    def _preprocess(self, template):
        """Modify template string before code conversion"""
        # Replace inline '%' blocks for easier parsing
        o = re.compile(r"(?m)^[ \t]*%((if|for|while|try).+:)")
        c = re.compile(r"(?m)^[ \t]*%(((else|elif|except|finally).*:)|(end\w+))")
        template = c.sub(r"<%:\g<1>%>", o.sub(r"<%\g<1>%>", template))

        # Replace {{!x}} and {{x}} variables with '<%echo(x)%>'.
        # If auto-escaping is enabled, uses echo(escape(x)) for the second.
        vars = r"\{\{\s*\!(.*?)\}\}", r"\{\{(.*?)\}\}"
        subs = [r"<%echo(\g<1>)%>\n"] * 2
        if self.options["escape"]: subs[1] = r"<%echo(escape(\g<1>))%>\n"
        for v, s in zip(vars, subs): template = re.sub(v, s, template)

        return template

    def _process(self, template):
        """Return the code generated from the template string"""
        code_blk = re.compile(r"<%(.*?)%>\n?", re.DOTALL)
        indent = 0
        code = []
        for n, blk in enumerate(code_blk.split(template)):
            # Replace '<\%' and '%\>' escapes
            blk = re.sub(r"<\\%", "<%", re.sub(r"%\\>", "%>", blk))
            # Unescape '%{}' characters
            blk = re.sub(r"\\(%|{|})", "\g<1>", blk)

            if not (n % 2):
                if not blk: continue # for n, blk
                # Escape double-quote characters
                blk = re.sub(r"\"", "\\\"", blk)
                blk = (" " * (indent*4)) + 'echo("""{0}""")'.format(blk)
            else:
                blk = blk.rstrip()
                if blk.lstrip().startswith(":"):
                    if not indent:
                        err = "unexpected block ending"
                        raise SyntaxError("Line {0}: {1}".format(n, err))
                    indent -= 1
                    if blk.startswith(":end"):
                        continue
                    blk = blk.lstrip()[1:]

                blk = re.sub("(?m)^", " " * (indent * 4), blk)
                if blk.endswith(":"):
                    indent += 1

            code.append(blk)

        if indent:
            err = "Reached EOF before closing block"
            raise EOFError("Line {0}: {1}".format(n, err))

        return "\n".join(code)

    def _postprocess(self, output):
        """Modify output string after variables and code evaluation"""
        if self.options["strip"]:
            output = Template.RE_STRIP.sub("", output)
        if self.options["collapse"]:
            output = collapse_whitespace(output)
        return output


def escape_html(x):
    """Escape HTML special characters &<> and quotes "'."""
    CHARS, ENTITIES = "&<>\"'", ["&amp;", "&lt;", "&gt;", "&quot;", "&#39;"]
    string = x if isinstance(x, text_types) else str(x)
    for c, e in zip(CHARS, ENTITIES): string = string.replace(c, e)
    return string


def to_unicode(x, encoding="utf-8"):
    """Convert anything to Unicode."""
    if PY3:
        return str(x)
    if not isinstance(x, unicode):
        x = unicode(str(x), encoding, errors="replace")
    return x


def collapse_whitespace(x):
    """
    Collapse whitespace into a single space globally,
    and into nothing if between non-alphanumerics.
    """
    x = re.sub(r"\s+", " ", x)
    x = re.sub(r"(\W)\s+(\W)", r"\1\2",  x, re.U)
    return x
