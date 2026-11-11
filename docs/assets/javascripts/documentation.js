import mermaid from "../vendor/mermaid/mermaid.esm.min.mjs";
import elkLayouts from "../vendor/elk/mermaid-layout-elk.esm.min.mjs";

window.canoeRender = {ready: false, errors: []};
mermaid.registerLayoutLoaders(elkLayouts);
mermaid.initialize({startOnLoad: false, securityLevel: "loose", maxTextSize: 200000,
  theme: "default", fontFamily: "Arial, sans-serif"});

async function renderDocumentation() {
  try {
    await MathJax.startup.promise;
    await MathJax.typesetPromise();
    for (const image of document.querySelectorAll("p > img[src*='assets/figures/']")) {
      if (!image.complete) await new Promise((resolve, reject) => {
        image.addEventListener("load", resolve, {once: true});
        image.addEventListener("error", () => reject(new Error(`Figure failed to load: ${image.src}`)), {once: true});
      });
      if (!image.naturalWidth) throw new Error(`Empty figure: ${image.src}`);
      const paragraph = image.parentElement;
      const caption = paragraph.nextElementSibling;
      const figure = document.createElement("figure");
      figure.className = "canoe-published-figure";
      paragraph.before(figure);
      figure.append(paragraph);
      if (caption?.tagName === "P" && caption.textContent.trim().startsWith("Figure ")) {
        const label = document.createElement("figcaption");
        label.innerHTML = caption.innerHTML;
        figure.append(label);
        caption.remove();
      }
      if (image.naturalWidth > 1000 || image.naturalHeight > 1000) {
        figure.classList.add(image.naturalHeight > image.naturalWidth ? "portrait-figure" : "wide-figure");
      }
    }
    const nodes = [...document.querySelectorAll(".canoe-mermaid")];
    for (const [index, node] of nodes.entries()) {
      const source = node.textContent;
      const {svg, bindFunctions} = await mermaid.render(`canoe-diagram-${index}`, source);
      node.innerHTML = svg;
      node.dataset.rendered = "true";
      if (bindFunctions) bindFunctions(node);
      let preceding = node.previousElementSibling;
      const introduction = [];
      while (preceding && introduction.length < 3) {
        if (/^H[234]$/.test(preceding.tagName)) {
          introduction.push(preceding);
          break;
        }
        if (preceding.tagName !== "P" || preceding.textContent.length > 600) {
          introduction.length = 0;
          break;
        }
        introduction.push(preceding);
        preceding = preceding.previousElementSibling;
      }
      if (!introduction.some(element => /^H[234]$/.test(element.tagName))) introduction.length = 0;
      const wrapper = document.createElement("div");
      wrapper.className = "canoe-diagram-page";
      node.before(wrapper);
      introduction.reverse().forEach(element => wrapper.append(element));
      wrapper.append(node);
      const box = node.querySelector("svg").viewBox.baseVal;
      if (box.width < 1000 && box.height < 800) wrapper.classList.add("compact-diagram");
    }
    window.canoeRender = {ready: true, errors: [], diagrams: nodes.length,
      equations: document.querySelectorAll("mjx-container").length};
  } catch (error) {
    window.canoeRender.errors.push(String(error));
    console.error("Documentation rendering failed", error);
  }
}
renderDocumentation();
