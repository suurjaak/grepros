
/** Attaches event handlers to all .toggle links.  */
var bindToggles = function() {
  document.querySelectorAll("a.toggle").forEach(function(link) {
    link.addEventListener("click", onToggle.bind(null, link));
    link.title = "Click to toggle section";
  });
};

/** Toggles link parent open or closed. */
var onToggle = function(link) {
  link.classList.toggle("open");
  link.parentElement.classList.toggle("open");
  return false;
}
