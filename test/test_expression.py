#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test: expression tree parsing.

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     17.01.2024
@modified    31.01.2024
------------------------------------------------------------------------------
"""
import collections
import itertools
import logging
import os
import re
import string
import sys

import six

from grepros import ExpressionTree


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test import testbase

logger = logging.getLogger()


VALIDS = [  # Evalable as formatted, and as raw when lowercased
    "a",
    "a AND b",
    "a AND b AND (c OR d) AND e",
    "a AND (b OR c OR (d AND e))",
    "a OR NOT (f AND g) ",
    "(d OR e) AND (a OR b)",
    "NOT a AND NOT (b OR c) AND d ",
    "((a AND ((b OR (c)))))",
    "a AND b OR c OR (d AND (e OR f))",
    "NOt noT a",
    "NOT NOT a OR b",
    "NOT a OR NOT b OR NOT c",
    "NOT a OR NOT b OR NOT NOT NOT (c OR d) OR NOT NOT e",
    "((NOT a) OR NOT b) OR NOT ((NOT NOT(c OR d) OR NOT NOT e))",
    "a OR NOT (NOT b OR NOT c OR NOT ((d)))",
    "NOT NOT NOT NOT NOT a",
    "a and (b or not c)",
    "a AND b OR c AND d OR e AND f OR g AND h",
    "a OR b AND c OR d",
    "a OR b AND c OR d AND (e OR f AND NOT (g OR h)) OR i AND (j OR k AND l)",
    "a OR (b) AND c",
    "a OR (b OR c AND (d OR (e OR f AND g) AND h)) AND NOT i",
    "NOT NOT NOT NOT NOT ((a)) AND NOT NOT NOT NOT NOT (b OR c)",
]

NO_RAWEVALS = {  # Evalable as formatted but not raw, as {expr: [..expected tree..]}
    'a OR "b"':           ["OR", [["VAL", "a"], ["VAL", "b"]]],
    "'a'" '"b"':          ["AND", [["VAL", "a"], ["VAL", "b"]]],
    "a NOT b":            ["AND", [["VAL", "a"], ["NOT", [["VAL", "b"]]]]],
    "'a'AND b":           ["AND", [["VAL", "a"], ["VAL", "b"]]],
    "a AND 'b' AND c":    ["AND", [["VAL", "a"], ["AND", [["VAL", "b"], ["VAL", "c"]]]]],
    "(a b) (c)":          ["AND", [["AND", [["VAL", "a"], ["VAL", "b"]]], ["VAL", "c"]]],
    "((a ((b OR (c)))))": ["AND", [["VAL", "a"], ["OR", [["VAL", "b"], ["VAL", "c"]]]]],
    "a b AND (c d)":      ["AND", [["VAL", "a"], ["AND", [["VAL", "b"],
                                                          ["AND", [["VAL", "c"], ["VAL", "d"]]]]]]],
}

NO_EVALS = {  # Not naively evalable, as {expr: [..expected tree..]}
    "'(a)' OR b":                 ["OR", [["VAL", "(a)"], ["VAL", "b"]]],
    "a 'b or c'":                 ["AND", [["VAL", "a"], ["VAL", "b or c"]]],
    "a 'b or c or \\'d \\ d\\''": ["AND", [["VAL", "a"], ["VAL", "b or c or 'd \\ d'"]]],
    "a 'b or \\'\"\\\"\\''":      ["AND", [["VAL", "a"], ["VAL", 'b or \'""\'']]],
    ''' "'\\"\\"'" ''':           ["VAL", '\'""\''],
}

INVALIDS = [  # Expressions that should fail tree parsing
    "((()))",
    "a OR NOT ()",
    "(",
    ")",
    "a AND ((b OR c)",
    "a AND (b OR c))",
    "AND",
    "AND OR NOT",
    "a AND OR NOT b",
    "a AND 'b",
    "a AND b'",
    "\\'a'",
]

EAGER_EVALS = {  # {expr: {op: {varname: expected access count to check for each namespace}}}
    "a AND b OR c AND d": {ExpressionTree.OR:  dict(a=1, c=1),
                           ExpressionTree.AND: dict(a=1, b=1)},
    "a OR b AND c OR d":  {ExpressionTree.OR:  dict(a=1, b=1)},
    "a OR b OR c":        {ExpressionTree.OR:  dict(a=1, b=1, c=1)},
    "a AND b AND c":      {ExpressionTree.AND: dict(a=1, b=1, c=1)},
}

INDEXED_TERMINALS = {  # {expr: [..expected tree with terminals as variable index..]}
    "a OR b AND c":   ["OR", [["VAL", 0], ["AND", [["VAL", 1], ["VAL", 2]]]]],
    "a AND (b OR c)": ["AND", [["VAL", 0], ["OR", [["VAL", 1], ["VAL", 2]]]]],
}


class TestExpressionTree(testbase.TestBase):

    ## Test name used in flow logging
    NAME = os.path.splitext(os.path.basename(__file__))[0]

    ## Name used in flow logging
    OUTPUT_LABEL = "console"


    def __init__(self, *args, **kwargs):
        super(TestExpressionTree, self).__init__(*args, **kwargs)
        self._expressor = ExpressionTree()
        self._cmd = ["grepros", '(this OR that) AND NOT "not"', "--expression",
                     "--match-wrapper", "--color", "never", "--path", self.DATA_DIR]


    def test_expressiontree(self):
        """Verifies ExpressionTree parsing and evaluating."""
        logger.info("Verifying parse & full eval.")
        for text in VALIDS:
            with self.subTest(text): self.verify_expression(text)
        logger.info("Verifying parse & formatted eval.")
        for text, tree in NO_RAWEVALS.items():
            with self.subTest(text): self.verify_expression(text, tree, texteval=False)
        logger.info("Verifying parse of quoted expressions.")
        for text, tree in NO_EVALS.items():
            with self.subTest(text):
                self.verify_expression(text, tree, expreval=False, texteval=False)
        logger.info("Verifying rejection of invalid expressions.")
        for text in INVALIDS:
            ERR = "Unexpected success from parsing invalid expression."
            with self.subTest(text):
                with self.assertRaises(ValueError, msg=ERR): self.verify_expression(text)
        logger.info("Verifying empty expressions.")
        with self.subTest("empty text"):
            for text in ("", " ", "\n", " \n\t \t\n\r"):
                self.assertFalse(self._expressor.parse(text), "Unexpected result for empty text.")
        logger.info("Verifying biiig expression.")
        with self.subTest("huge text"):
            text = " AND ".join(["(a OR b)"] * 10000)
            self.verify_expression(text)


    def test_expressiontree_eager(self):
        """Verifies operator shortcircuit overrides."""
        logger.info("Verifying eval with eager operators.")
        FULLS = [x for x in VALIDS if re.search("and|or", x, re.I) and not ("'" in x or '"' in x)]
        for text in FULLS:
            with self.subTest("eager eval of %r" % text): self.verify_eager(text)
        for text, ops in EAGER_EVALS.items():
            with self.subTest("eager eval of %r" % text): self.verify_eager(text, ops)


    def test_expressiontree_terminal(self):
        """Verifies operand terminal usage in parse and format."""
        logger.info("Verifying terminals in parse and format.")
        for text, tree in INDEXED_TERMINALS.items():
            with self.subTest(text):
                self.verify_terminal(text, tree)


    def test_grepros_cli_expression(self):
        """Runs grepros on bags in data directory, verifies console output."""
        logger.info("Verifying expression use in command-line.")
        self.verify_bags()
        fulltext = self.run_command()
        self.assertTrue(fulltext, "Command did not print to console.")

        for bag in self._bags:
            self.assertIn(os.path.basename(bag), fulltext, "Expected bag not in output.")
        self.verify_topics(topics=fulltext, messages=fulltext)


    def verify_expression(self, text, expected=None, expreval=True, texteval=True):
        """Verifies expression parsing and evaluating, raises on error."""
        logger.debug("Verifying expression %r%s.", *(text, "") if len(text) < 100 else
                     (text[:100].strip( ) + "...", " [%s chars]" % len(text)))
        tree = self._expressor.parse(text)
        self.assertIsInstance(tree, list, "Unexpected result type from parse().")
        self.assertEqual(expected, tree, "Unexpected result from parse().") \
            if expected is not None else None

        expr = self._expressor.format(tree)
        self.assertIsInstance(expr, six.text_type, "Unexpected result type from format().")
        if not expreval and not texteval:
            try: compile(expr, "", "eval")
            except Exception: self.fail("Error compiling formatted expression."); raise
            return

        safetext = re.sub("and|or|not", lambda m: m[0].upper(), text, flags=re.I)
        EVALS = dict(dict(formatted=expr) if expreval else {}, **dict(raw=text) if texteval else {})
        VARS = "".join(x for x in string.ascii_lowercase if x in safetext)  # Eval all value combos
        for ns in (dict(zip(VARS, x)) for x in itertools.product([True, False], repeat=len(VARS))):
            treeresult = self._expressor.evaluate(tree, terminal=ns.get)
            evalresults = {}
            for name, txt in EVALS.items():
                try: evalresults[name] = eval(txt.lower(), dict(ns))
                except Exception:
                    logger.exception("Error evaluating %s text (namespace %s).", name, ns)
                    raise
            for name, txt in EVALS.items():
                self.assertEqual(treeresult, evalresults[name],
                                 "Eval mismatch: tree vs Python eval of %s text." % name)
            if len(evalresults) > 1:
                self.assertEqual(evalresults["raw"], evalresults["formatted"],
                                 "Eval mismatch: raw vs formatted Python eval.")


    def verify_eager(self, text, ops=None):
        """Verifies expression evaluating values in full despite short-circuit."""
        logger.debug("Verifying eager evaluation of expression %r.", text)
        listify = lambda x: x if isinstance(x, list) else [x]
        safetext = re.sub("and|or|not", lambda m: m[0].upper(), text, flags=re.I)
        tree = self._expressor.parse(text)

        EAGERS = ops or [[k for k in self._expressor.BINARIES if k in safetext]]
        VARS = "".join(x for x in string.ascii_lowercase if x in safetext)  # Eval all value combos
        for ns in (dict(zip(VARS, x)) for x in itertools.product([True, False], repeat=len(VARS))):
            for eager in EAGERS:
                counter = collections.Counter()
                terminal = lambda x: ns.get(x, counter.update(x))
                treeresult = self._expressor.evaluate(tree, terminal=terminal, eager=listify(eager))
                for var in (ops[eager] if ops else VARS):
                    expected = ops[eager][var] if ops else safetext.count(var)
                    self.assertEqual(expected, counter[var],
                                     "Variable %r not accessed as eagerly as expected." % var)
                evalresult = eval(text.lower(), dict(ns))
                self.assertEqual(treeresult, evalresult,
                                 "Eval mismatch: eager tree vs Python eval of text.")


    def verify_terminal(self, text, expected):
        """Verifies ExpressionTree.parse() and .format() using terminal callbacks."""
        logger.debug("Verifying terminaled evaluation of expression %r.", text)
        safetext = re.sub("and|or|not", lambda m: m[0].upper(), text, flags=re.I)
        VARS = "".join(x for x in string.ascii_lowercase if x in safetext)

        tree = self._expressor.parse(text, terminal=VARS.index)  # Replace name with index
        self.assertEqual(expected, tree, "Unexpected result from parse().")

        expr = self._expressor.format(tree, terminal=VARS.__getitem__)  # Replace index with name
        self.assertEqual(text.lower(), expr, "Unexpected result from format().")



if "__main__" == __name__:
    TestExpressionTree.run_rostest()
