<%
"""
HTML export template.

@param   source     inputs.SourceBase instance
@param   sink       inputs.HtmlSink instance
@param   args       list of command-line arguments
@param   timeline   whether to create timeline
@param   messages   iterable yielding (topic, index, stamp, msg, match)

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     06.11.2021
@modified    25.11.2021
------------------------------------------------------------------------------
"""
import datetime, os, re
from grepros import __version__

dt =  datetime.datetime.now().strftime("%Y-%m-%d %H:%M") 
sourcemeta = source.get_meta()
subtitle = os.path.basename(sourcemeta["file"]) if "file" in sourcemeta else "live"
%>
<!DOCTYPE HTML><html lang="">
<head>
  <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
  <meta name="generator" content="grepros {{ __version__ }}" />
  <title>grepros {{ subtitle }} {{ dt }}</title>
  <style>
    body {
      background:             #300A24;
      color:                  white;
      font-family:            monospace;
      position:               relative;
    }
    #header #meta {
      white-space:            pre-wrap;
    }
    #footer {
      color:                  gray;
      bottom:                 0;
      position:               absolute;
    }
    #content {
      padding-bottom:         20px;
    }
    .hidden {
      display:                none;
    }
    table#messages {
      border-collapse:        collapse;
    }
    table#messages tr {
      border:                 1px solid gray;
    }
    table#messages tr.meta {
      border-bottom-width:    0;
      border-top-width:       1px;
      color:                  gray;
    }
    table#messages tr.message {
      border-top-width:       0;
    }
    table#messages tr.meta.collapsed {
      border-bottom-width:    1px;
      cursor:                 pointer;
    }
    table#messages tr.meta.collapsed span.index {
      display:                none;
    }
    table#messages tr.meta span.index {
      display:                block;
      opacity:                0.5;
    }
    table#messages tr.message.collapsed {
      display:                none;
    }
    table td, table th {
      text-align:             left;
      vertical-align:         top;
    }
    #topics {
      margin-bottom:          20px;
    }
    #topics > span:first-child {
      cursor:                 pointer;
    }
    table#toc {
      margin-top:             10px;
    }
    table#toc td, table#toc th {
      padding:                0px 5px;
      position:               relative;
    }
    table#toc td:nth-child(4), table#toc th:nth-child(4) {
      text-align:             right;
    }
    table#toc input[type=checkbox] {
      height:                 10px;
      width:                  10px;
    }
    table#toc.collapsed {
      display:                none;
    }
    table#toc tbody:empty::after {
      content:                "Loading..";
      color:                  gray;
      position:               relative;
      left:                   10px;
    }
    th .sort {
      display:                block;
    }
    th .sort:hover {
      cursor:                 pointer;
      text-decoration:        none;
    }
    th .sort::after {
      content:                "";
      display:                inline-block;
      min-width:              6px;
      position:               relative;
      left:                   3px;
      top:                    -1px; 
    }
    th .sort.asc::after {
      content:                "\\2193";  /** Downwards arrow ↓. */
    }
    th .sort.desc::after {
      content:                "\\2191";  /** Upwards arrow ↓. */
    }
    table#messages td, table#messages th {
      padding:                5px 10px;
      position:               relative;
    }
    span.data {
      white-space:            pre-wrap;
    }
    span.match {
      color:                  red;
    }
    span.lowlight {
      color:                  gray;
    }
    span.comment {
      color:                  darkgreen;
    }
    span.separator {
      color:                  dimgray;
    }
    table#messages span.prev::after, table#messages span.next::after {
      color:                  cornflowerblue;
      cursor:                 pointer;
      display:                block;
      position:               absolute;
      text-align:             center;
      top:                    5px;
      width:                  15px;
    }
    table#messages span.prev::after {
      content:                "\\21E1";  /* Upwards dashed arrow ⇡. */
      right:                  20px;
    }
    table#messages span.next::after {
      content:                "\\21E3";  /* Downwards dashed arrow ⇣. */
      right:                  5px;
    }
    table#messages span.disabled::after {
      color:                  gray;
      cursor:                 default;
    }
    span.toggle::after {
      content:                "–";
      cursor:                 pointer;
    }
    span.toggle.collapsed::after,
    tr.collapsed span.toggle::after {
      content:                "+";
    }
    a {
      color:                  cornflowerblue;
      text-decoration:        none;
    }
    a:hover {
      text-decoration:        underline;
      text-decoration-style:  dotted;
    }
    @keyframes flash_border {
      0%   { background-color: inherit;  }
      50%  { background-color: #450F35; }
      100% { background-color: inherit; }
    }
    #messages tr.highlight {
      animation: flash_border 1s 1 linear;
    }
%if timeline:
    #timeline {
      background:             rgba(66, 14, 51, 0.8);
      position:               fixed;
      top:                    0px;
      bottom:                 0px;
      right:                  0px;
      padding:                5px;
      z-index:                1;
    }
    #timeline span.toggle {
      position:               absolute;
      top:                    5px;
      right:                  5px;
    }
    #timeline ul {
      list-style-type:        none;
      margin:                 0;
      padding:                1px;
      position:               relative;
      overflow-y:             auto;
      overflow-x:             hidden;
    }
    #timeline ul li {
      line-height:            25px;
    }
    #timeline ul li a {
      padding-right:          10px;
    }
    #timeline ul li a:hover {
      cursor:                 pointer;
      text-decoration:        underline;
      text-decoration-style:  dotted;
    }
    #timeline ul li span.time {
      color:                  gray;
    }
    #timeline ul li .count {
      float:                  right;
      font-size:              0.8em;
      color:                  lightgray;
    }
    #timeline.collapsed {
      bottom:                 unset;
    }
    #timeline.collapsed ul {
      height:                 0;
      overflow:               hidden;
    }
%endif
    #overlay {
      display:                flex;
      align-items:            center;
      bottom:                 0;
      justify-content:        center;
      left:                   0;
      position:               fixed;
      right:                  0;
      top:                    0;
      z-index:                10000;
    }
    #overlay.hidden {
      display:                none;
    }
    #overlay #overshadow {
      background:             black;
      opacity:                0.5;
      position:               fixed;
      bottom:                 0;
      left:                   0;
      right:                  0;
      top:                    0;
      height:                 100%;
      width:                  100%;
    }
    #overlay #overcontent {
      background:             gray;
      display:                flex;
      flex-direction:         column;
      justify-content:        space-between;
      max-height:             calc(100% - 100px);
      max-width:              calc(100% - 100px);
      opacity:                1;
      padding:                10px;
      padding-right:          20px;
      position:               relative;
      z-index:                10001;
    }
    #overlay #overlayclose {
      position:               absolute;
      top:                    0;
      right:                  3px;
      color:                  black;
      cursor:                 pointer;
    }
    #overlay .content {
      margin:                 10px 0 10px 0;
      padding:                0 10px 0 0;
      overflow:               auto;
      white-space:            pre-wrap;
    }
  </style>
  <script>
    var TOPICS    = {};  // {topic: [type, ]}
    var TOPICKEYS = [];  // [[topic, type], ]
    var SCHEMAS   = {};  // {type: schema}
    var FIRSTMSGS = {};  // {[topic, type]: {id, dt}}
    var LASTMSGS  = {};  // {[topic, type]: {id, dt}}
    var MSGCOUNTS = {};  // {[topic, type]: count}
%if timeline:
    var MSGIDS    = [];  // [id, ]
    var MSGSTAMPS = [];  // [dtstr, ] indexes correspond to MSGIDS
%endif

    var sort_col       = 0;     // Current sort column index in toc
    var sort_direction = true;  // Ascending


    /**
     * Adds positional formatting with {#index}-parameters to JavaScript String.
     * Function parameters are invoked as String.replace function argument.
     */
    String.prototype.format = String.prototype.f = function() {
        var s = this, i = arguments.length;
        while (i--) s = s.replace(new RegExp("\\\\{" + i + "\\\\}", "g"), arguments[i]);
        return s;
    };


%if timeline:
    /** Adds right-justifying to JavaScript String. */
    String.prototype.rjust = function(len, char) {
      len = this.length < len ? len - this.length : 0;
      char = char || " ";
      return String(this + Array(len+1).join(char));
    };


    /** Adds left-justifying to JavaScript String. */
    String.prototype.ljust = function(len, char) {
      len = this.length < len ? len - this.length : 0;
      char = char || " ";
      return String(Array(len+1).join(char) + this);
    };


    /** Adds basic numeric strftime formatting to JavaScript Date, local time. */
    Date.prototype.strftime = function(format) {
      var result = format.replace(/%%/g, "~~~~~~~~~~"); // Temp-replace literal %
      var x = this;
      var FMTERS = {
        "%d": function() { return String(x.getDate()).ljust(2, "0"); },
        "%m": function() { return String(x.getMonth() + 1).ljust(2, "0"); },
        "%y": function() { return String(x.getFullYear() % 100).ljust(2, "0").slice(-2); },
        "%Y": function() { return String(x.getFullYear()).rjust(4, "0"); },
        "%H": function() { return String(x.getHours()).ljust(2, "0"); },
        "%I": function() { var h = x.getHours(); return String(h > 12 ? h - 12 : h).ljust(2, "0"); },
        "%M": function() { return String(x.getMinutes()).ljust(2, "0"); },
        "%S": function() { return String(x.getSeconds()).ljust(2, "0"); },
        "%f": function() { return String(x.getMilliseconds()).rjust(3, "0").ljust(6, "0"); },
        "%w": function() { return x.getDay(); },
        "%W": function() { return String(Util.weekOfYear(x)).ljust(2, "0"); },
        "%p": function() { return (x.getHours() < 12) ? "AM" : "PM"; },
        "%P": function() { return (x.getHours() < 12) ? "am" : "pm"; },
      };
      for (var f in FMTERS) result = result.replace(new RegExp(f, "g"), FMTERS[f]);
      return result.replace(/~~~~~~~~~~/g, "%"); // Restore literal %-s
    };


%endif
    /** Registers entry in a topic. */
    function registerTopic(topic, type, schema, id, dt) {
      var topickey = [topic, type];
      TOPICKEYS.push(topickey);
      SCHEMAS[type] = schema;
      registerMessage(null, id, dt, topic, type);
    }


    /** Registers message. */
    function registerMessage(topicindex, id, dt, topic, type) {
      var topickey = (topic && type) ? [topic, type] : TOPICKEYS[topicindex];
      if (!FIRSTMSGS[topickey]) {
        FIRSTMSGS[topickey] = {"id": id, "dt": dt};
        (TOPICS[topic] = TOPICS[topic] || []).push(type);
      };
      LASTMSGS[topickey] = {"id": id, "dt": dt};
      MSGCOUNTS[topickey] = (MSGCOUNTS[topickey] || 0) + 1;
%if timeline:
      MSGIDS.push(id);
      MSGSTAMPS.push(dt);
%endif
    }


    /** Shows popup with message type definition. */
    function showSchema(type, evt) {
      var elem = document.getElementById("overlay");
      elem.classList.remove("hidden");
      var text = SCHEMAS[type].split("\\n").map(function(line) {
        if (!line.match(/^w?string [^#]*=/))  // String constant lines cannot have comments
          return line.replace(/#.*$/g, '<span class="comment">$&</span>');
        return line;
      }).join("\\n");
      text = text.replace(/^(=+)$/gm, '<span class="separator">$1</span>');
      text = text.replace(/\\n/g, "<br />");
      elem.getElementsByClassName("title")[0].innerText = type + ":";
      elem.getElementsByClassName("content")[0].innerHTML = text;
      evt && evt.stopPropagation && evt.stopPropagation();
      return false;
    }


    /** Toggles message collapsed or maximized. */
    function toggleMessage(id, evt) {
      var elem_meta = document.getElementById(id);
      var elem_msg  = document.getElementById(id + "msg");
      elem_meta.classList.toggle("collapsed");
      elem_msg .classList.toggle("collapsed");
      evt && evt.stopPropagation && evt.stopPropagation();
      return false;
    }


    /** Handler for clicking message header, uncollapses if collapsed. */
    function onMessageHeader(id, event) {
      var elem = document.getElementById(id);
      if (elem.classList.contains("collapsed")) toggleMessage(id, event);
    }


    /** Toggles all messages collapsed or maximized. */
    function toggleAllMessages(evt) {
      var elem_table = document.getElementById("messages");
      var rows = elem_table.getElementsByTagName("tr");
      var method = (rows[0].classList.contains("collapsed")) ? "remove" : "add";
      [].slice.call(rows).forEach(function(v) { v.classList[method]("collapsed"); });
      evt && evt.stopPropagation && evt.stopPropagation();
      return false;
    }


    /** Toggles class on ID-d element, and toggle-element if any. */
    function toggleClass(id, cls, elem_toggle) {
      var elem = document.getElementById(id);
      if (elem) {
        elem.classList.toggle(cls);
        if (elem_toggle) elem_toggle.classList.toggle(cls);
      };
      return false;
    }


    /** Toggles overlay popup on or off. */
    var toggleOverlay = function(evt) {
      document.getElementById("overlay").classList.toggle("hidden");
      evt && evt.preventDefault();
    };


    /** Goes to previous or next message in same topic. */
    function gotoSibling(id_source, direction, elem_link) {
      if (elem_link && elem_link.classList.contains("disabled")) return;
      var elem_source = document.getElementById(id_source);
      if (elem_source) {
        var selectors = [].slice.call(elem_source.classList).filter(function(v) { return v.match(/\\//); });
        var adjacent = (direction < 0) ? "previousElementSibling" : "nextElementSibling";
        var elem_sibling = elem_source[adjacent];
        var found = false;
        while (elem_sibling) {
          if (selectors.every(function(v) { return elem_sibling.classList.contains(v); })) {
            gotoMessage(elem_sibling.id);
            found = true;
            break;  // while
          };
          elem_sibling = elem_sibling[adjacent];
        }
        if (!found && elem_link) {
          elem_link.classList.add("disabled");
          elem_link.title = "No more messages in topic";
        };
      };
      return false;
    }


    /** Scrolls to specified message, unhides and uncollapses it, and highlights it briefly. */
    function gotoMessage(id) {
      var elem_meta = document.getElementById(id);
      var elem_msg  = document.getElementById(id + "msg");
      if (elem_meta && elem_msg) {
        ["collapsed", "hidden", "highlight"].forEach(function(cls) {
          elem_meta.classList.remove(cls); elem_msg.classList.remove(cls);
        });
        elem_meta.scrollIntoView();
        window.requestAnimationFrame && window.requestAnimationFrame(function() {
          elem_meta.classList.add("highlight"); elem_msg.classList.add("highlight");
          window.setTimeout(function() {
            elem_meta.classList.remove("highlight"); elem_msg.classList.remove("highlight");
          }, 1500);
        });
      };
      return false;
    }


    /** Shows or hides all message rows. */
    function enableTopics(elem_cb) {
      var toggler = elem_cb.checked ? "remove" : "add";
      var rows = document.querySelectorAll("#messages tbody tr");
      [].slice.call(rows).forEach(function(x) { x.classList[toggler]("hidden"); });
      var cbs = document.querySelectorAll("#toc tbody input[type=checkbox]");
      [].slice.call(cbs).forEach(function(x) { x.checked = elem_cb.checked; });
    }


    /** Shows or hides topic message rows. */
    function enableTopic(topic, type, elem_cb) {
      var toggler = elem_cb.checked ? "remove" : "add";
      var selectors = [topic, type].filter(Boolean).map(function(x) { return x.replace(/[^\w\-]/g, "\\\$&"); });
      var rows = document.querySelectorAll("#messages tbody tr.meta");
      for (var i = 0; i < rows.length; i++) {
        var elem_tr = rows[i];
        if (selectors.every(function(v) { return elem_tr.classList.contains(v); })) {
          elem_tr.classList[toggler]("hidden");
          selectors.length && elem_tr.nextElementSibling.classList[toggler]("hidden");
        };
      };
    }


    /** Returns a new DOM tag with specified body (text or node, or list of) and attributes. */
    function createElement(name, body, opts) {
      var result = document.createElement(name);
      var elems = Array.isArray(body) ? body : [body];
      for (var i = 0; i < elems.length; i++) {
        var elem = elems[i];
        if (elem != null && "object" != typeof(elem)) elem = document.createTextNode(elem);
        if (elem != null) result.appendChild(elem);
      };
      Object.keys(opts || {}).forEach(function(x) { result.setAttribute(x, opts[x]); });
      return result;
    };


    /** Sorts table of contents by column. */
    function sort(col) {
      var elem_table = document.getElementById("toc");
      var elem_tbody = elem_table.getElementsByTagName("tbody")[0];
      var elems_tr   = elem_tbody.getElementsByTagName("tr");
      if (!elems_tr.length) return false;

      if (col == sort_col && !sort_direction)
        sort_col = 0, sort_direction = true;
      else if (col == sort_col)
        sort_direction = !sort_direction;
      else
        sort_col = col, sort_direction = true;

      var elems_tr = elem_tbody.getElementsByTagName("tr");
      var rows = [];
      for (var i = 0, ll = elems_tr.length; i != ll; rows.push(elems_tr[i++]));
      rows.sort(cmp_node);
      for (var i = 0; i < rows.length; i++) elem_tbody.appendChild(rows[i]);
      var linklist = document.getElementsByClassName("sort");
      for (var i = 0; i < linklist.length; i++) {
        linklist[i].classList.remove("asc");
        linklist[i].classList.remove("desc");
        if (i + 1 == sort_col) linklist[i].classList.add(sort_direction ? "asc" : "desc")
      };
      return false;
    };


    /** Returns node child content comparison result, -1, 0 or 1. */
    var cmp_node = function(a, b) {
      var v1 = a.children[sort_col] ? a.children[sort_col].innerText.toLowerCase() : "";
      var v2 = b.children[sort_col] ? b.children[sort_col].innerText.toLowerCase() : "";
      var result = String(v1).localeCompare(String(v2), undefined, {numeric: true});
      return cmp(v1, v2) * (sort_direction ? 1 : -1);
    };


    /** Returns string comparison result, -1, 0 or 1. */
    var cmp = function(a, b) {
      return String(a).localeCompare(String(b), undefined, {numeric: true});
    };


    /** Builds the table of contents. */
    var populateToc = function() {
      var elem_table = document.getElementById("toc");
      elem_table.querySelector("th").appendChild(
        createElement("input", null, {"type": "checkbox", "checked": "checked",
                                      "title": "Toggle visibility of all topic messages",
                                      "onclick": "return enableTopics(this)"})
      );

      var elem_tbody = elem_table.getElementsByTagName("tbody")[0];
      Object.keys(TOPICS).sort(cmp).forEach(function(topic, i) {
        TOPICS[topic].sort(cmp).forEach(function(type, j) {
          var topickey = [topic, type];
          var id0 = FIRSTMSGS[topickey]["id"], id1 = LASTMSGS[topickey]["id"];
          var dt0 = FIRSTMSGS[topickey]["dt"], dt1 = LASTMSGS[topickey]["dt"];
          var elem_row = document.createElement("tr");
          var id_cb = "cb_topic_{0}_{1}".format(i, j);
          var elem_cb = createElement("input", null, {"type": "checkbox", "checked": "checked", "id": id_cb,
                                                      "title": "Toggle topic messages visibility",
                                                      "onclick": "return enableTopic('{0}', '{1}', this)".format(topic, type)});
          elem_row.appendChild(createElement("td", elem_cb));
          elem_row.appendChild(createElement("td", createElement("label", topic, {"for": id_cb})));
          var elem_type = createElement("a", type, {"href": "javascript:;", "title": "Show type definition",
                                                    "onclick": "return showSchema('{0}')".format(type)});
          elem_row.appendChild(createElement("td", elem_type));
          elem_row.appendChild(createElement("td", (MSGCOUNTS[topickey]).toLocaleString("en")));
          var elem_first = createElement("a", dt0, {"href": "#" + id0, "title": "Go to first message in topic",
                                                    "onclick": "return gotoMessage('{0}')".format(id0)});
          elem_row.appendChild(createElement("td", elem_first))
          if (id0 != id1) {
            var elem_last = createElement("a", dt1, {"href": "#" + id1, "title": "Go to last message in topic",
                                                     "onclick": "return gotoMessage('{0}')".format(id1)});
            elem_row.appendChild(createElement("td", elem_last))
          };
          elem_tbody.appendChild(elem_row);
        });
      });
    };


%if timeline:
    /** Populates timeline structures and the timeline element. */
    var populateTimeline = function() {
      var NUM = 30;  // @todo from window height mebbe

      // Calculate a sensibly rounded interval
      var entryspan = (new Date(MSGSTAMPS[MSGSTAMPS.length - 1]) - new Date(MSGSTAMPS[0])) / NUM;
      var ROUNDINGS = [ // [[threshold in millis, rounding in millis], ]
        [3600 * 1000, 3600 * 1000],  // >1h, round to 60m
        [ 600 * 1000,  900 * 1000],  // 10m..60m, round to 15m
        [ 300 * 1000,  600 * 1000],  // 5m..10m, round to 10m
        [ 120 * 1000,  300 * 1000],  // 2m..5m, round to 5m
        [  60 * 1000,  120 * 1000],  // 1m..2m, round to 2m
        [  30 * 1000,   60 * 1000],  // 30s..1m, round to 1m
        [  10 * 1000,   30 * 1000],  // 10s..30s, round to 30s
        [   5 * 1000,   10 * 1000],  // 5s..10s, round to 10s
        [   2 * 1000,    5 * 1000],  // 2s..5s, round to 5s
        [   1 * 1000,    2 * 1000],  // 1s..2s, round to 2s
        [        500,    1 * 1000],  // 500ms..1s, round to 1s
        [        100,         100],  // 100ms..500ms, round to 100
        [          5,          10],  // 5ms..100ms, round to 10
        [          2,           5],  // 2ms..5ms, round to 5
        [          1,           1],  // 1ms..2ms, round to 2
      ];
      var rounded = false;
      for (var i = 0; i < ROUNDINGS.length; i++) {
        var threshold = ROUNDINGS[i][0], modulo = ROUNDINGS[i][1];
        if (entryspan > threshold) {
          entryspan += modulo - entryspan % modulo;
          rounded = true;
          break;  // for i
        };
      };
      if (!rounded) entryspan = 1000;

      // Assemble timeline entries
      var timeentry = new Date(MSGSTAMPS[0]);
      timeentry = new Date(+timeentry - (+timeentry % entryspan));
      var timelines = [timeentry];
      var indexids = {};  // [dt: first message ID]
      var counts   = {};  // {dt: message count}
      for (var i = 0; i < MSGSTAMPS.length; i++) {
        var dt = new Date(MSGSTAMPS[i]);
        if (!i || +dt >= +timeentry + +entryspan) {
          while (i && +dt >= +timeentry + +entryspan) {
            timeentry = new Date(+timeentry + +entryspan);
            timelines.push(timeentry);
          };
          indexids[timeentry] = MSGIDS[i];
        };
        counts[timeentry] = (counts[timeentry] || 0) + 1;
      };

      // Generate date format string
      var dt1 = new Date(MSGSTAMPS[0]);
      var dt2 = new Date(MSGSTAMPS[MSGSTAMPS.length - 1]);
      var fmtstr = "";
      if (dt1.getYear() != dt2.getYear()) {
        fmtstr = "%Y-%m-%d";
      } else if (dt1.getMonth() != dt2.getMonth() || dt1.getDate() != dt2.getDate()) {
        fmtstr = "%m-%d";
      };
      fmtstr += (fmtstr ? " " : "") + "%H:%M:%S";
      if (dt1.strftime(fmtstr) == dt2.strftime(fmtstr)) fmtstr += ".%f";

      // Populate timeline
      var elem_timeline = document.querySelector("#timeline ul");
      for (var i = 0; i < timelines.length; i++) {
        var dt = timelines[i];
        var attr = !counts[dt] ? {"class": "time"} :
                                 {"title": "Go to first message in time span", "href": "javascript;",
                                  "onclick": "return gotoMessage({0})".format(indexids[dt])};
        var elem_time  = createElement(counts[dt] ? "a" : "span", dt.strftime(fmtstr), attr);
        var elem_count = counts[dt] ? createElement("span", (counts[dt]).toLocaleString("en"), {"class": "count"}) : null;
        var elem_li    = createElement("li", [elem_time, elem_count]);
        elem_timeline.appendChild(elem_li);
      };
      elem_timeline.parentNode.classList.remove("hidden");
    };


%endif
    window.addEventListener("DOMContentLoaded", function() {
      populateToc();
%if timeline:
      populateTimeline();
%endif
    });

    if (document.location.hash) document.location.hash = "";

  </script>
</head>


<body>

<div id="header">
  <div id="meta">{{ source.format_meta().strip() }}
Command: {{ " ".join(args) }}
  </div>
  <div id="topics">
    <span title="Toggle contents" onclick="return toggleClass('toc', 'collapsed', document.getElementById('toggle_topics'))">
      Contents: <span id="toggle_topics" class="toggle collapsed"></span>
    </span>
    <table id="toc" class="collapsed">
      <thead>
        <tr>
          <th></th>
%for i, name in enumerate(["topic", "type", "count", "first", "last"], 1):
          <th><a class="sort" href="javascript:;" title="Sort by {{ name }}" onclick="sort({{ i + 1 }})">{{ name.capitalize() }}</span></th>
%endfor
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>


<div id="overlay" class="hidden">
  <div id="overshadow"></div>
  <div id="overcontent">
    <span id="overlayclose" title="Close">x</span>
    <div class="title"></div>
    <div class="content"></div>
  </div>
</div>
<script>
  document.getElementById("overlayclose").addEventListener("click", toggleOverlay);
  document.getElementById("overshadow").addEventListener("click", toggleOverlay);
  document.body.addEventListener("keydown", function(evt) {
    if (evt.keyCode == 27 && !document.getElementById("overlay").classList.contains("hidden")) toggleOverlay();
  });
</script>


<div id="timeline" class="hidden">
  Timeline:
  <span title="Toggle timeline" class="toggle" onclick="return toggleClass('timeline', 'collapsed', this)"></span>
  <ul></ul>
</div>


<div id="footer">Written by grepros on {{ dt }}.</div>


<div id="content">
  <table id="messages">
    <thead><tr>
      <th>#</th>
      <th>Topic</th>
      <th>Type</th>
      <th>Datetime</th>
      <th>Timestamp</th>
      <th><span class="toggle all" title="Toggle all messages" onclick="return toggleAllMessages()"></span></th>
    </tr></thead>
    <tbody>
<%
topic_idx = {}  # {(topic, type), ]
selector = lambda v: re.sub(r"([^\w\-])", r"\\\1", v)
%>
%for i, (topic, index, stamp, msg, match) in enumerate(messages, 1):
    <%
meta = source.get_message_meta(topic, index, stamp, msg)
topickey = (topic, meta["type"])
    %>
    <tr class="meta {{ selector(topic) }} {{ selector(meta["type"]) }}" id="{{ i }}" onclick="return onMessageHeader({{ i }}, event)">
      <td>
        {{ "{0:,d}".format(index) }}{{ ("/{0:,d}".format(meta["total"])) if "total" in meta else "" }}
        <span class="index">{{ i }}</span>
      </td>
      <td>{{ topic }}</td>
      <td><a title="Show type definition" href="javascript:;" onclick="return showSchema('{{ meta["type"] }}', event)">{{ meta["type"] }}</td>
      <td>{{ meta["dt"] }}</td>
      <td>{{ meta["stamp"] }}</td>
      <td>
        <span class="toggle" title="Toggle message" onclick="return toggleMessage({{ i }}, event)"></span>
      </td>
    </tr>
    <tr id="{{ i }}msg" class="message">
      <td></td>
      <td colspan="5">
        <span class="data">{{! sink.format_message(match or msg, highlight=bool(match)) }}</span>
    %if topickey in topic_idx:
        <span class="prev" title="Go to previous message"
              onclick="return gotoSibling({{ i }}, -1, this)"></span>
    %endif
        <span class="next" title="Go to next message in topic"
              onclick="return gotoSibling({{ i }}, +1, this)"></span>
    %if topickey in topic_idx:
        <script> registerMessage('{{ topic_idx[topickey] }}', {{ i }}, '{{ meta["dt"] }}'); </script>
    %else:
        <script> registerTopic('{{ topic }}', '{{ meta["type"] }}', '{{ meta.get("schema", "").replace("\n", "\\n") }}', {{ i }}, '{{ meta["dt"] }}'); </script>
    %endif
      </td>
    </tr>
    <%
topic_idx.setdefault(topickey, len(topic_idx))
    %>
%endfor
    </tbody>
  </table>
</div>

</body>
</html>
