<%
"""
HTML export template.

@param   source     inputs.SourceBase instance
@param   sink       inputs.HtmlSink instance
@param   args       list of command-line arguments
@param   messages   iterable yielding (topic, index, stamp, msg, match)

------------------------------------------------------------------------------
This file is part of grepros - grep for ROS bag files and live topics.
Released under the BSD License.

@author      Erki Suurjaak
@created     06.11.2021
@modified    07.11.2021
------------------------------------------------------------------------------
"""
import datetime, re

dt =  datetime.datetime.now().strftime("%Y-%m-%d %H:%M") 
%>
<!DOCTYPE HTML><html lang="">
<head>
  <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
  <meta name="generator" content="grepros 0.2.1" />
  <title>grepros {{ dt }}</title>
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
    #topics ul {
      margin:                 0;
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
    table#toc td, table#toc th {
      padding:                0px 5px;
      position:               relative;
    }
    table#toc td:nth-child(3), table#toc th:nth-child(3) {
      text-align:             right;
    }
    table#toc.collapsed {
      display:                none;
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
    span.comment {
      color:                  darkgreen;
    }
    span.separator {
      color:                  dimgray;
    }
    table#messages span.prev::after, table#messages span.next::after {
      color:                  cornflowerblue;
      cursor:                 pointer;
      position:               absolute;
      right:                  10px;
    }
    table#messages span.prev::after {
      content:                "\\21E1";  /* Upwards dashed arrow ⇡. */
      top:                    -5px;
    }
    table#messages span.next::after {
      content:                "\\21E3";  /* Downwards dashed arrow ⇣. */
      bottom:                 5p;
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
    }
  </style>
  <script>
    var TOPICS    = {};  // {topic: [type, ]}
    var SCHEMAS   = {};  // {type: schema}
    var FIRSTMSGS = {};  // {[topic, type]: {id, dt}}
    var LASTMSGS  = {};  // {[topic, type]: {id, dt}}
    var MSGCOUNTS = {};  // {[topic, type]: count}


    /**
     * Adds positional formatting with {#index}-parameters to JavaScript String.
     * Function parameters are invoked as String.replace function argument.
     */
    String.prototype.format = String.prototype.f = function() {
        var s = this, i = arguments.length;
        while (i--) s = s.replace(new RegExp("\\\\{" + i + "\\\\}", "g"), arguments[i]);
        return s;
    };


    /** Registers entry in a topic. */
    function registerTopic(topic, type, schema, id, dt) {
      var topickey = [topic, type];
      SCHEMAS[type] = schema;
      if (!FIRSTMSGS[topickey]) {
        FIRSTMSGS[topickey] = {"id": id, "dt": dt};
        (TOPICS[topic] = TOPICS[topic] || []).push(type);
      };
      LASTMSGS[topickey] = {"id": id, "dt": dt};
      MSGCOUNTS[topickey] = (MSGCOUNTS[topickey] || 0) + 1;
    }


    /** Shows popup with message type definition. */
    function showSchema(type, evt) {
      var elem = document.getElementById("overlay");
      elem.classList.remove("hidden");
      var text = SCHEMAS[type].replace(/#[^\\n]+/g, '<span class="comment">$&</span>');
      text = text.replace(/\\n(=+)\\n/g, '\\n<span class="separator">$1</span>\\n');
      text = text.replace(/\\n/g, "<br />");
      elem.getElementsByClassName("title")[0].innerText = type + ":";
      elem.getElementsByClassName("content")[0].innerHTML = text;
      evt && evt.stopPropagation && evt.stopPropagation();
      return false;
    }


    /** Toggles message on or off. */
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


    /** Toggles all messages on or off. */
    function toggleAllMessages(evt) {
      var elem_table = document.getElementById("messages");
      var rows = elem_table.getElementsByTagName("tr");
      var method = (rows[0].classList.contains("collapsed")) ? "remove" : "add";
      [].slice.call(rows).forEach(function(v) { v.classList[method]("collapsed"); });
      evt && evt.stopPropagation && evt.stopPropagation();
      return false;
    }


    /** Toggles all messages on or off. */
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


    /** Scrolls to a preceding or following sibling element, and uncollapses it. */
    function gotoSibling(id_source, sibling_classes, direction) {
      var elem_source = document.getElementById(id_source);
      if (elem_source) {
        var selectors = sibling_classes.map(function(v) { return v.replace(/[^\w\-]/g, "\\\$&"); });
        var adjacent = (direction < 0) ? "previousElementSibling" : "nextElementSibling";
        var elem_sibling = elem_source[adjacent];
        while (elem_sibling) {
          if (selectors.every(function(v) { return elem_sibling.classList.contains(v); })) {
            if (elem_sibling.classList.contains("collapsed")) toggleMessage(elem_sibling.id);
            elem_sibling.scrollIntoView();
            break;  // while
          };
          elem_sibling = elem_sibling[adjacent];
        }
      };
      return false;
    }


    /** Scrolls to specified message, and uncollapses it. */
    function gotoMessage(id) {
      var elem = document.getElementById(id);
      if (elem) {
        if (elem.classList.contains("collapsed")) toggleMessage(id);
        elem.scrollIntoView();
      };
      return false;
    }


    /** Returns a new DOM tag with specified inner HTML and attributes. */
    function createElement(name, html, opts) {
      var result = document.createElement(name);
      if ("object" == typeof(html)) result.append(html);
      else if (html != null) result.innerHTML = html;
      Object.keys(opts || {}).forEach(function(x) { result.setAttribute(x, opts[x]); });
      return result;
    };


    if (document.location.hash) document.location.hash = "";
    window.addEventListener("load", function() {
      var elem_table = document.getElementById("toc");
      var elem_tbody = elem_table.getElementsByTagName("tbody")[0];
      Object.keys(TOPICS).sort().forEach(function(topic) {
        TOPICS[topic].sort().forEach(function(type) {
          var topickey = [topic, type];
          var id0 = FIRSTMSGS[topickey]["id"], dt0 = FIRSTMSGS[topickey]["dt"];
          var id1 = LASTMSGS [topickey]["id"], dt1 = LASTMSGS [topickey]["dt"];
          var elem_row = document.createElement("tr");
          elem_row.append(createElement("td", topic));
          var elem_type = createElement("a", type, {"href": "javascript:;", "onclick": "return showSchema('{0}')".format(type)});
          elem_row.append(createElement("td", elem_type));
          elem_row.append(createElement("td", (MSGCOUNTS[topickey]).toLocaleString("en")));
          var elem_first = createElement("a", dt0, {"href": "#" + id0, "onclick": "return gotoMessage('{0}')".format(id0)});
          elem_row.append(createElement("td", elem_first))
          if (id0 != id1) {
            var elem_last = createElement("a", dt1, {"href": "#" + id1, "onclick": "return gotoMessage('{0}')".format(id1)});
            elem_row.append(createElement("td", elem_last))
          };
          elem_tbody.append(elem_row);
        });
      });
      if (elem_table.classList.contains("collapsed")) {
        toggleClass("toc", "collapsed", elem_table.parentElement.getElementsByTagName("span")[0]);
      };
    });
  </script>
</head>


<body>

<div id="header">
  <div id="meta">{{ source.format_meta().strip() }}
Command: {{ " ".join(args) }}
  </div>
  <div id="topics">
    Contents: <span class="toggle collapsed" title="Toggle contents" onclick="return toggleClass('toc', 'collapsed', this)"></span>
    <table id="toc" class="collapsed">
      <thead>
        <tr>
          <th>Topic</th>
          <th>Type</th>
          <th>Count</th>
          <th>First</th>
          <th>Last</th>
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


<div id="footer">Written by grepros on {{ dt }}.</div>


<div id="content">
  <table id="messages">
    <tr>
      <th>#</th>
      <th>Topic</th>
      <th>Type</th>
      <th>Datetime</th>
      <th>Timestamp</th>
      <th><span class="toggle all" title="Toggle all messages" onclick="return toggleAllMessages()"></span></th>
    </tr>
<%
topics_seen = set()  # {(topic, type), ]
selector = lambda v: re.sub(r"([^\w\-])", r"\\\1", v)
%>
%for i, (topic, index, stamp, msg, match) in enumerate(messages, 1):
    <%
meta = source.get_message_meta(topic, index, stamp, msg)
topickey = (topic, meta["type"])
    %>
    <tr class="meta {{ selector(topic) }} {{ selector(meta["type"]) }}" id="{{ i }}" onclick="return onMessageHeader({{ i }}, event)">
      <td>{{ "{0:,d}".format(index) }}{{ ("/{0:,d}".format(meta["total"])) if "total" in meta else "" }}</td>
      <td>{{ topic }}</td>
      <td><a title="Show schema" href="javascript:;" onclick="return showSchema('{{ meta["type"] }}', event)">{{ meta["type"] }}</td>
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
    %if topickey in topics_seen:
        <span class="prev" title="Go to previous message in topic '{{ topic }}'"
              onclick="return gotoSibling({{ i }}, ['{{ topic }}', '{{ meta["type"] }}'], -1)"></span>
    %endif
        <span class="next" title="Go to next message in topic '{{ topic }}'"
              onclick="return gotoSibling({{ i }}, ['{{ topic }}', '{{ meta["type"] }}'], +1)"></span>
        <script> registerTopic('{{ topic }}', '{{ meta["type"] }}', '{{ meta.get("schema", "").replace("\n", "\\n") }}', {{ i }}, '{{ meta["dt"] }}'); </script>
      </td>
    </tr>
    <%
topics_seen.add(topickey)
    %>
%endfor
  </table>
</div>

</body>
</html>
