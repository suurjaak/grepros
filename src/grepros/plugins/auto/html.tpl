<%
"""
HTML export template.

@param   source     inputs.Source instance
@param   sink       inputs.HtmlSink instance
@param   args       list of command-line arguments, if any
@param   timeline   whether to create timeline
@param   messages   iterable yielding (topic, msg, stamp, match, index)

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     06.11.2021
@modified    03.07.2023
------------------------------------------------------------------------------
"""
import datetime, os, re
from grepros import __title__, __version__, api

dt =  datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
sourcemeta = source.get_meta()
subtitle = os.path.basename(sourcemeta["file"]) if "file" in sourcemeta else "live"
%>
<!DOCTYPE HTML><html lang="">
<head>
  <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
  <meta name="generator" content="{{ __title__ }} {{ __version__ }}" />
  <title>{{ __title__ }} {{ subtitle }} {{ dt }}</title>
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
    #timeline > span:first-child {
      cursor:                 pointer;
      display:                block;
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
    #timeline ul li.highlight {
      filter:                 brightness(1.1);
      font-weight:            bold;
    }
    #timeline ul li.highlight .count {
      color:                  white;
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

    /**
     * Adds positional formatting with {#index}-parameters to JavaScript String.
     * Function parameters are invoked as String.replace function argument.
     */
    String.prototype.format = String.prototype.f = function() {
        var s = this, i = arguments.length;
        while (i--) s = s.replace(new RegExp("\\\\{" + i + "\\\\}", "g"), arguments[i]);
        return s;
    };


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
      var result = format.replace(/%%/g, "~~~~~~~~~~");  // Temp-replace literal %
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
        "%f": function() { return String(Math.floor(x.getNanoseconds() / 1000)).ljust(6, "0"); },
        "%F": function() { return String(x.getMilliseconds()).ljust(3, "0"); },
        "%n": function() { return String(x.getNanoseconds()).ljust(9, "0"); },
        "%w": function() { return x.getDay(); },
        "%W": function() { return String(Util.weekOfYear(x)).ljust(2, "0"); },
        "%p": function() { return (x.getHours() < 12) ? "AM" : "PM"; },
        "%P": function() { return (x.getHours() < 12) ? "am" : "pm"; },
      };
      for (var f in FMTERS) result = result.replace(new RegExp(f, "g"), FMTERS[f]);
      return result.replace(/~~~~~~~~~~/g, "%");  // Restore literal %-s
    };


    /** Adds nanosecond support to JavaScript Date. */
    Date.prototype.setNanoseconds = function(ns) {
      this._nanoseconds = ns;
      this.setMilliseconds(ns / 1000000);
    };
    Date.prototype.getNanoseconds = function() {
      if (this._nanoseconds === undefined)
        this._nanoseconds = this.getMilliseconds() * 1000000;
      return this._nanoseconds;
    };


    /** Returns a new DOM tag with specified body (text or node, or list of) and attributes. */
    var createElement = function(name, body, opts) {
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


    /**
     * Returns formatted timestamp for [seconds, nanoseconds],
     * format defaulting to "%Y-%m-%d %H:%M:%S.%f",
     * with trailing zeros stripped from ending fractional seconds.
     */
    var formatStamp = function(secs_nsecs, format) {
      format = format || "%Y-%m-%d %H:%M:%S.%f";
      var result = makeDate(secs_nsecs).strftime(format);
      return RegExp("((%f)|(%n))$", "i").test(format) ? result.replace(/\\.?0+$/, "") : result;
    };


    /** Returns full height of an element, including paddings, margins, and border. */
    var getOuterHeight = function(elem) {
      // This is deliberately excluding margin for our calculations, since we are using
      // offsetTop which is including margin.
      var height = elem.clientHeight;
      var computedStyle = window.getComputedStyle(elem);
      height += parseInt(computedStyle.borderTopWidth, 10);
      height += parseInt(computedStyle.borderBottomWidth, 10);
      height += parseInt(computedStyle.marginTop, 10);
      height += parseInt(computedStyle.marginBottom, 10);
      return height;
    };


    /** Returns element bounding box height: paddings, margins, and border. */
    var getBoxHeight = function(elem) {
      var height = elem.clientHeight;
      var computedStyle = window.getComputedStyle(elem);
      height -= parseInt(computedStyle.paddingTop, 10);
      height -= parseInt(computedStyle.paddingBottom, 10);
      return getOuterHeight(elem) - height;
    };


    /** Returns JavaScript Date with nanosecond precision. */
    var makeDate = function(secs_nsecs) {
      var dt = new Date(secs_nsecs[0] * 1000);
      dt.setNanoseconds(secs_nsecs[1]);
      return dt;
    };


    /** Toggles class on ID-d element, and toggle-element if any. */
    var toggleClass = function(id, cls, elem_toggle) {
      var elem = document.getElementById(id);
      if (elem) {
        elem.classList.toggle(cls);
        if (elem_toggle) elem_toggle.classList.toggle(cls);
      };
      return false;
    };


    /** Encapsulates messages table handling. */
    var Messages = new function() {
      var self = this;

      var TOPICS    = self.TOPICS    = {};  // {topic: [[typename, typehash], ]}
      var TOPICKEYS = self.TOPICKEYS = [];  // [[topic, typename, typehash], ]
      var SCHEMAS   = self.SCHEMAS   = {};  // {[typename, typehash]: schema}
      var FIRSTMSGS = self.FIRSTMSGS = {};  // {[topic, typename, typehash]: {id, dt}}
      var LASTMSGS  = self.LASTMSGS  = {};  // {[topic, typename, typehash]: {id, dt}}
      var MSGCOUNTS = self.MSGCOUNTS = {};  // {[topic, typename, typehash]: count}


      /** Registers entry in a topic. */
      self.registerTopic = function(topic, type, hash, schema, id, secs_nsecs) {
        var topickey = [topic, type, hash];
        TOPICKEYS.push(topickey);
        SCHEMAS[[type, hash]] = schema;
        self.registerMessage(null, id, secs_nsecs, topic, type, hash);
      };


      /** Registers message. */
      self.registerMessage = function(topicindex, id, secs_nsecs, topic, type, hash) {
        var topickey = (topicindex != null) ? TOPICKEYS[topicindex] : [topic, type, hash];
        var dt = formatStamp(secs_nsecs);
        if (!FIRSTMSGS[topickey]) {
          FIRSTMSGS[topickey] = {"id": id, "dt": dt};
          (TOPICS[topic] = TOPICS[topic] || []).push([type, hash]);
        };
        LASTMSGS[topickey] = {"id": id, "dt": dt};
        MSGCOUNTS[topickey] = (MSGCOUNTS[topickey] || 0) + 1;
        Timeline && Timeline.registerMessage(id, secs_nsecs);
      };


      /** Shows popup with message type definition. */
      self.showSchema = function(type, hash, evt) {
        var elem = document.getElementById("overlay");
        elem.classList.remove("hidden");
        var text = SCHEMAS[[type, hash]].split("\\n").map(function(line) {
          if (!line.match(/^w?string [^#]*=/))  // String constant lines cannot have comments
            return line.replace(/#.*$/g, '<span class="comment">$&</span>');
          return line;
        }).join("\\n");
        text = text.replace(/^(=+)$/gm, '<span class="separator">$1</span>');
        text = text.replace(/\\n/g, "<br />");
        elem.getElementsByClassName("title")[0].innerText = "{0}:".format(type);
        elem.getElementsByClassName("content")[0].innerHTML = text;
        evt && evt.stopPropagation && evt.stopPropagation();
        return false;
      };


      /** Toggles message collapsed or maximized. */
      self.toggleMessage = function(id, evt) {
        var elem_meta = document.getElementById(id);
        var elem_msg  = document.getElementById(id + "msg");
        elem_meta.classList.toggle("collapsed");
        elem_msg .classList.toggle("collapsed");
        evt && evt.stopPropagation && evt.stopPropagation();
        return false;
      };


      /** Handler for clicking message header, uncollapses if collapsed. */
      self.onClickHeader = function(id, event) {
        var elem = document.getElementById(id);
        if (elem.classList.contains("collapsed")) self.toggleMessage(id, event);
      };


      /** Toggles all messages collapsed or maximized. */
      self.toggleAllMessages = function(evt) {
        var elem_table = document.getElementById("messages");
        var rows = elem_table.getElementsByTagName("tr");
        var method = (rows[0].classList.contains("collapsed")) ? "remove" : "add";
        [].slice.call(rows).forEach(function(v) { v.classList[method]("collapsed"); });
        evt && evt.stopPropagation && evt.stopPropagation();
        return false;
      };


      /** Toggles overlay popup on or off. */
      self.toggleOverlay = function(evt) {
        document.getElementById("overlay").classList.toggle("hidden");
        evt && evt.preventDefault();
      };


      /** Goes to previous or next message in same topic. */
      self.gotoSibling = function(id_source, direction, elem_link) {
        if (elem_link && elem_link.classList.contains("disabled")) return;
        var elem_source = document.getElementById(id_source);
        if (elem_source) {
          var SKIPCLS = ["meta", "message", "collapsed", "hidden", "highlight"];
          var selectors = [].slice.call(elem_source.classList).filter(function(v) { return SKIPCLS.indexOf(v) < 0; });
          var adjacent = (direction < 0) ? "previousElementSibling" : "nextElementSibling";
          var elem_sibling = elem_source[adjacent];
          var found = false;
          while (elem_sibling) {
            if (selectors.every(function(v) { return elem_sibling.classList.contains(v); })) {
              self.gotoMessage(elem_sibling.id);
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
      };


      /** Scrolls to specified message, unhides and uncollapses it, and highlights it briefly. */
      self.gotoMessage = function(id) {
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
      };

    };


    /** Encapsulates table of contents handling. */
    var TOC = new function() {
      var self = this;

      var sort_col       = 1;     // Current sort column index in toc
      var sort_direction = true;  // Ascending


      /** Shows or hides all message rows. */
      self.enableTopics = function(elem_cb) {
        var toggler = elem_cb.checked ? "remove" : "add";
        var rows = document.querySelectorAll("#messages tbody tr");
        [].slice.call(rows).forEach(function(x) { x.classList[toggler]("hidden"); });
        var cbs = document.querySelectorAll("#toc tbody input[type=checkbox]");
        [].slice.call(cbs).forEach(function(x) { x.checked = elem_cb.checked; });
      }


      /** Shows or hides topic message rows. */
      self.enableTopic = function(topic, type, hash, elem_cb) {
        var toggler = elem_cb.checked ? "remove" : "add";
        var selectors = [topic, type, hash].filter(Boolean).map(function(x) { return x.replace(/[^\w\-]/g, "\\\$&"); });
        var rows = document.querySelectorAll("#messages tbody tr.meta");
        for (var i = 0; i < rows.length; i++) {
          var elem_tr = rows[i];
          if (selectors.every(function(v) { return elem_tr.classList.contains(v); })) {
            elem_tr.classList[toggler]("hidden");
            selectors.length && elem_tr.nextElementSibling.classList[toggler]("hidden");
          };
        };
      }


      /** Sorts table of contents by column. */
      self.sort = function(col) {
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
        rows.sort(self.cmp_node);
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
      self.cmp_node = function(a, b) {
        var v1 = a.children[sort_col] ? a.children[sort_col].innerText.toLowerCase() : "";
        var v2 = b.children[sort_col] ? b.children[sort_col].innerText.toLowerCase() : "";
        var result = String(v1).localeCompare(String(v2), undefined, {numeric: true});
        return self.cmp(v1, v2) * (sort_direction ? 1 : -1);
      };


      /** Returns string comparison result, -1, 0 or 1. */
      self.cmp = function(a, b) {
        return String(a).localeCompare(String(b), undefined, {numeric: true});
      };


      /** Builds the table of contents. */
      self.init = function() {
        var elem_table = document.getElementById("toc");
        elem_table.querySelector("th").appendChild(
          createElement("input", null, {"type": "checkbox", "checked": "checked",
                                        "title": "Toggle visibility of all topic messages",
                                        "onclick": "return TOC.enableTopics(this)"})
        );

        var elem_tbody = elem_table.getElementsByTagName("tbody")[0];
        Object.keys(Messages.TOPICS).sort(self.cmp).forEach(function(topic, i) {
          Messages.TOPICS[topic].sort(self.cmp).forEach(function(typekey, j) {
            var type = typekey[0], hash = typekey[1];
            var topickey = [topic, type, hash];
            var id0 = Messages.FIRSTMSGS[topickey]["id"], id1 = Messages.LASTMSGS[topickey]["id"];
            var dt0 = Messages.FIRSTMSGS[topickey]["dt"], dt1 = Messages.LASTMSGS[topickey]["dt"];
            var elem_row = document.createElement("tr");
            var id_cb = "cb_topic_{0}_{1}".format(i, j);
            var elem_cb = createElement("input", null, {"type": "checkbox", "checked": "checked", "id": id_cb,
                                                        "title": "Toggle topic messages visibility",
                                                        "onclick": "return TOC.enableTopic('{0}', '{1}', '{2}', this)".format(topic, type, hash)});
            elem_row.appendChild(createElement("td", elem_cb));
            elem_row.appendChild(createElement("td", createElement("label", topic, {"for": id_cb})));
            var elem_type = createElement("a", type, {"href": "javascript:;", "title": "Show type definition",
                                                      "onclick": "return Messages.showSchema('{0}', '{1}')".format(type, hash)});
            elem_row.appendChild(createElement("td", elem_type));
            elem_row.appendChild(createElement("td", (Messages.MSGCOUNTS[topickey]).toLocaleString("en")));
            var elem_first = createElement("a", dt0, {"href": "#" + id0, "title": "Go to first message in topic",
                                                      "onclick": "return Messages.gotoMessage('{0}')".format(id0)});
            elem_row.appendChild(createElement("td", elem_first))
            if (id0 != id1) {
              var elem_last = createElement("a", dt1, {"href": "#" + id1, "title": "Go to last message in topic",
                                                       "onclick": "return Messages.gotoMessage('{0}')".format(id1)});
              elem_row.appendChild(createElement("td", elem_last))
            };
            elem_tbody.appendChild(elem_row);
          });
        });
      };

    };


    /** Encapsulates messages timeline handling. */
    var Timeline = new function() {
      var self = this;

      var MSGIDS            = [];    // [id, ]
      var MSGSTAMPS         = [];    // [[secs, nanosecs], ] indexes correspond to MSGIDS
      var MSG_TIMELINES     = {};    // {message ID: timeline index}
      var scroll_highlights = {};    // {row ID: highlight}
      var scroll_timer      = null;  // setTimeout ID
      var scroll_timeline   = true;  // Whether to scroll timeline on scrolling messages
      var scroll_history    = [window.scrollY, window.scrollY]; // Last scroll positions


      /** Populates timeline and sets up scroll highlighting. */
      self.init = function() {
        self.populate();
        self.initHighlight();

        var timeout = null;
        var height  = window.innerHeight;
        window.addEventListener("resize", function() {
          clearTimeout(timeout);
          timeout = setTimeout(function() {
            if (height != window.innerHeight) self.populate();
            height = window.innerHeight;
          }, 250);
        });
      };


      /** Registers message ID and timestamp for timeline. */
      self.registerMessage = function(id, secs_nsecs) {
        MSGIDS.push(id);
        MSGSTAMPS.push(secs_nsecs);
      };


      /** Returns a sensibly rounded interval for timeline steps, as nanoseconds. */
      self.calculateInterval = function(count) {
        var st1 = MSGSTAMPS[0], stN = MSGSTAMPS[MSGSTAMPS.length - 1];  // [seconds, nanoseconds],
        var st2 = MSGSTAMPS[Math.min(1, MSGSTAMPS.length - 1)];
        var entryspan_ns = ((stN[0] - st1[0]) * 10**9 + (stN[1] - st1[1])) / count;  // Ensure max precision

        var ROUNDINGS = [ // [[threshold in nanos, rounding in nanos], ]
          [3600 * 1000000000, 3600 * 1000000000],  // >1h, round to 60m
          [ 600 * 1000000000,  900 * 1000000000],  // 10m..60m, round to 15m
          [ 300 * 1000000000,  600 * 1000000000],  // 5m..10m, round to 10m
          [ 120 * 1000000000,  300 * 1000000000],  // 2m..5m, round to 5m
          [  60 * 1000000000,  120 * 1000000000],  // 1m..2m, round to 2m
          [  30 * 1000000000,   60 * 1000000000],  // 30s..1m, round to 1m
          [  10 * 1000000000,   30 * 1000000000],  // 10s..30s, round to 30s
        ];
        var MAGNITUDE_ROUNDINGS = [ // [[seconds/millis/micros/nanos threshold, rounding], ]
          [5,   10],    // 5..10, round to 10
          [2,    5],    // 2..5, round to 5
          [1,    2],    // 1..2, round to 2
          [0.5,  1],    // 0.5..1, round to 1
          [0.1,  0.1],  // 0.1s..0.5, round to 0.1
        ];
        var rounded = false;
        var magnitude = null;
        while (!rounded && (!magnitude || magnitude >= 1)) {
          var roundings = magnitude ? MAGNITUDE_ROUNDINGS : ROUNDINGS;
          for (var i = 0; i < roundings.length; i++) {
            var threshold = roundings[i][0], modulo = roundings[i][1];
            if (magnitude) { threshold = threshold * magnitude, modulo = modulo * magnitude; };
            if (threshold >= 1 && entryspan_ns > threshold) {
              entryspan_ns += modulo - entryspan_ns % modulo;
              rounded = true;
              break;  // for i
            };
          };
          magnitude = (magnitude == null) ? 10**9 : magnitude / 1000;
        };
        if (!rounded) entryspan_ns = 1;

        return entryspan_ns;
      };


      /** Generates date format string with sufficient granularity for given timelines. */
      self.makeDateFormat = function(timelines) {
        // Generate date format string
        var dt1 = makeDate(timelines[0]);
        var dt2 = makeDate(timelines[Math.min(1, timelines.length - 1)]);
        var dtN = makeDate(timelines[timelines.length - 1]);
        var fmtstr = "";
        if (dt1.getYear() != dtN.getYear()) {
          fmtstr = "%Y-%m-%d";
        } else if (dt1.getMonth() != dtN.getMonth()) {
          fmtstr = "%m-%d";
        } else if (dt1.getDate() != dtN.getDate() || +dtN - +dt1 > 24 * 3600000) {
          fmtstr = "%m-%d";
        };
        fmtstr += (fmtstr ? " " : "") + "%H:%M:%S";
        if (dt1.strftime(fmtstr) == dt2.strftime(fmtstr)) {
          var fracts = ".%F";  // Millis
          if (dt1.strftime(fmtstr + fracts) == dt2.strftime(fmtstr + fracts)) fracts = ".%f";  // Micros
          if (dt1.strftime(fmtstr + fracts) == dt2.strftime(fmtstr + fracts)) fracts = ".%n";  // Nanos
          fmtstr += fracts;
        };
        return fmtstr;
      };


      /** Returns [s1+s2, ns1+ns2] from [s1, ns2] and [s2, ns2]. */
      self.add = function(secs_nsecs1, secs_nsecs2) {
        var s = secs_nsecs1[0] + secs_nsecs2[0], ns = secs_nsecs1[1] +  secs_nsecs2[1];
        return (ns > 10**9) ? [s + 1, ns - 10**9] : [s, ns];
      };


      /** Returns [secs, nsecs] rounded lower. */
      self.round = function(secs_nsecs, nsecs) {
        var s = secs_nsecs[0], ns = secs_nsecs[1];
        if (nsecs > 10**9) { s -= s % Math.floor(nsecs / 10**9); ns = 0; }
        else { ns -= ns % nsecs; };
        return [s, ns];
      };


      /** Returns whether first stamp is equal to or greater than the second. */
      self.is_not_less = function(secs_nsecs1, secs_nsecs2) {
        return secs_nsecs1[0] > secs_nsecs2[0] || (secs_nsecs1[0] == secs_nsecs2[0] && secs_nsecs1[1] > secs_nsecs2[1]);
      };


      /** Populates timeline structures and the timeline element. */
      self.populate = function() {
        var elem_container = document.querySelector("#timeline");
        var elem_timeline  = document.querySelector("#timeline ul");
        var elem_title     = document.querySelector("#timeline span");
        elem_container.classList.remove("hidden");

        var HEIGHT = window.innerHeight - getBoxHeight(elem_container) - getBoxHeight(elem_timeline) - getOuterHeight(elem_title);
        var NUM = Math.max(5, Math.floor(HEIGHT / 25));  // / li.height
        var entryspan_ns = self.calculateInterval(NUM);

        // Assemble timeline entries
        var timeentry = self.round(MSGSTAMPS[0], entryspan_ns);
        var entryspan = [Math.floor(entryspan_ns / 10**9), entryspan_ns % 10**9];
        var nextentry = self.add(timeentry, entryspan);
        var timelines = [timeentry];
        var indexids = {};  // [secs_nsecs: first message ID]
        var counts   = {};  // {secs_nsecs: message count}
        MSG_TIMELINES = {};
        for (var i = 0; i < MSGSTAMPS.length; i++) {
          var st = MSGSTAMPS[i];
          if (!i || self.is_not_less(st, nextentry)) {
            while (i && self.is_not_less(st, nextentry)) {
              timeentry = nextentry;
              timelines.push(timeentry);
              nextentry = self.add(timeentry, entryspan);
            };
            indexids[timeentry] = MSGIDS[i];
          };
          counts[timeentry] = (counts[timeentry] || 0) + 1;
          MSG_TIMELINES[MSGIDS[i]] = timelines.length - 1;
        };

        var fmtstr = self.makeDateFormat(timelines);

        // Populate timeline
        elem_timeline.innerHTML = "";
        for (var i = 0; i < timelines.length; i++) {
          var st = timelines[i];
          var attr = !counts[st] ? {"class": "time"} :
                                    {"title": "Go to first message in time span", "href": "javascript;",
                                     "onclick": "return Messages.gotoMessage({0})".format(indexids[st])};
          var elem_time  = createElement(counts[st] ? "a" : "span", makeDate(st).strftime(fmtstr), attr);
          var elem_count = counts[st] ? createElement("span", (counts[st]).toLocaleString("en"), {"class": "count"}) : null;
          var elem_li    = createElement("li", [elem_time, elem_count], {"id": "timeline_" + i});
          elem_timeline.appendChild(elem_li);
        };
      };


      /** Attaches scroll observer to message rows. */
      self.initHighlight = function() {
        if (!window.IntersectionObserver) return;

        var scroll_options = {"root": null, "threshold": [0, 1]};
        var scroll_observer = new IntersectionObserver(self.onScrollMessages, scroll_options);

        var items = Array.prototype.slice.call(document.querySelectorAll("#timeline ul li"));
        items.forEach(function(x) { x.children.length > 1 && x.addEventListener("click", self.onClick); });

        var rows = Array.prototype.slice.call(document.querySelectorAll("#messages tbody tr"));
        rows.forEach(scroll_observer.observe.bind(scroll_observer));
      };


      /** Highlights timeline for messages in view, scrolls to highlights. */
      self.highlight = function() {
        scroll_timer = null;

        var on = {}, off = {};    // {"timeline_x": true}
        var msg_highlights = {};  // {message ID: do_highlight}
        var msg_rows       = {};  // {message ID: [row ID, ]}
        var row_ids = Object.keys(scroll_highlights);
        for (var i = 0; i < row_ids.length; i++) {
          var msg_id = row_ids[i].replace(/\D/g, "");
          msg_highlights[msg_id] = msg_highlights[msg_id] || scroll_highlights[row_ids[i]];
          (msg_rows[msg_id] = msg_rows[msg_id] || []).push(row_ids[i]);
        };
        var msg_ids = Object.keys(msg_highlights);
        for (var i = 0; i < msg_ids.length; i++) {
          var msg_id = msg_ids[i], into_view = msg_highlights[msg_id];
          if (!into_view) for (var j = 0; j < msg_rows[msg_id].length; j++) {
            delete scroll_highlights[msg_rows[msg_id][j]];
          };
          (into_view ? on : off)["timeline_" + MSG_TIMELINES[msg_id]] = true;
        };
        Object.keys(on).forEach(function(x) { delete off[x]; });
        var elems_off = Object.keys(off).map(document.getElementById.bind(document)).filter(Boolean),
            elems_on  = Object.keys(on ).map(document.getElementById.bind(document)).filter(Boolean);
        elems_off.forEach(function(x) { x.classList.remove("highlight"); });
        elems_on. forEach(function(x) { x.classList.add   ("highlight"); });
        if (!scroll_timeline || !elems_on.length) return;

        var container = elems_on[0].parentElement,
            viewport  = [container.scrollTop, container.scrollTop + container.clientHeight],
            downward  = scroll_history[1] > scroll_history[0],
            anchor    = elems_on[downward ? elems_on.length - 1 : 0];
        anchor = (downward ? anchor.nextElementSibling : anchor.previousElementSibling) || anchor;
        if (anchor.offsetTop >= viewport[0] && anchor.offsetTop + anchor.offsetHeight <= viewport[1]) return;
        container.scrollTop = Math.max(0, anchor.offsetTop - (downward ? container.clientHeight - anchor.offsetHeight : 0));
      };


      /** Cancels scrolling timeline on ensuring messages-viewport change. */
      self.onClick = function() {
        scroll_timeline = false;
        window.setTimeout(function() { scroll_timeline = true; }, 500);
        return true;
      };


      /** Queues scrolled messages for highlighting timeline. */
      self.onScrollMessages = function(entries) {
        if (!document.querySelector("#timeline li")) return;

        scroll_history = [scroll_history[1], window.scrollY];
        if (scroll_timeline !== false)
          scroll_timeline = ("none" != document.getElementById("timeline").style.display) ? true : null;
        if (scroll_timeline === null) return;

        entries.forEach(function(entry) {
          scroll_highlights[entry.target.id] = entry.isIntersecting;
        });
        scroll_timer = scroll_timer || window.setTimeout(self.highlight , 100);
      };

    };


    window.addEventListener("DOMContentLoaded", function() {
      TOC.init()
%if timeline:
      Timeline.init();
%endif
    });

    if (document.location.hash) document.location.hash = "";

  </script>
</head>


<body>

<div id="header">
%if source.format_meta().strip() or isdef("args") and args:
  <div id="meta">{{ source.format_meta().strip() }}
    %if isdef("args") and args:
Command: {{ __title__ }} {{ " ".join(args) }}
    %endif
%endif
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
          <th><a class="sort" href="javascript:;" title="Sort by {{ name }}" onclick="TOC.sort({{ i }})">{{ name.capitalize() }}</span></th>
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
  document.getElementById("overlayclose").addEventListener("click", Messages.toggleOverlay);
  document.getElementById("overshadow").addEventListener("click", Messages.toggleOverlay);
  document.body.addEventListener("keydown", function(evt) {
    if (evt.keyCode == 27 && !document.getElementById("overlay").classList.contains("hidden")) Messages.toggleOverlay();
  });
</script>


<div id="timeline" class="hidden">
  <span title="Toggle timeline" onclick="return toggleClass('timeline', 'collapsed', document.getElementById('toggle_timeline'))">
    Timeline: <span id="toggle_timeline" class="toggle"></span>
  </span>
  <ul></ul>
</div>


<div id="footer">Written by {{ __title__ }} on {{ dt }}.</div>


<div id="content">
  <table id="messages">
    <thead><tr>
      <th>#</th>
      <th>Topic</th>
      <th>Type</th>
      <th>Datetime</th>
      <th>Timestamp</th>
      <th><span class="toggle all" title="Toggle all messages" onclick="return Messages.toggleAllMessages()"></span></th>
    </tr></thead>
    <tbody>
<%
topic_idx = {}  # {(topic, typename, typehash), ]
selector = lambda v: re.sub(r"([^\w\-])", r"\\\1", v)
%>
%for i, (topic, msg, stamp, match, index) in enumerate(messages, 1):
    <%
secs, nsecs = divmod(api.to_nsec(stamp), 10**9)
meta = source.get_message_meta(topic, msg, stamp, index)
topickey = (topic, meta["type"], meta["hash"])
    %>
    <tr class="meta {{ selector(topic) }} {{ selector(meta["type"]) }} {{ selector(meta["hash"]) }}" id="{{ i }}" onclick="return Messages.onClickHeader({{ i }}, event)">
      <td>
        {{ "{0:,d}".format(index) }}{{ ("/{0:,d}".format(meta["total"])) if "total" in meta else "" }}
        <span class="index">{{ i }}</span>
      </td>
      <td>{{ topic }}</td>
      <td><a title="Show type definition" href="javascript:;" onclick="return Messages.showSchema('{{ meta["type"] }}', '{{ meta["hash"] }}', event)">{{ meta["type"] }}</td>
      <td>{{ meta["dt"] }}</td>
      <td>{{ meta["stamp"] }}</td>
      <td>
        <span class="toggle" title="Toggle message" onclick="return Messages.toggleMessage({{ i }}, event)"></span>
      </td>
    </tr>
    <tr id="{{ i }}msg" class="message">
      <td></td>
      <td colspan="5">
        <span class="data">{{! sink.format_message(match or msg, highlight=bool(match)) }}</span>
    %if topickey in topic_idx:
        <span class="prev" title="Go to previous message"
              onclick="return Messages.gotoSibling({{ i }}, -1, this)"></span>
    %endif
        <span class="next" title="Go to next message in topic"
              onclick="return Messages.gotoSibling({{ i }}, +1, this)"></span>
    %if topickey in topic_idx:
        <script> Messages.registerMessage('{{ topic_idx[topickey] }}', {{ i }}, {{ [secs, nsecs] }}); </script>
    %else:
        <script> Messages.registerTopic('{{ topic }}', '{{ meta["type"] }}', '{{ meta["hash"] }}', '{{ meta.get("schema", "").replace("\n", "\\n") }}', {{ i }}, {{ [secs, nsecs] }}); </script>
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
