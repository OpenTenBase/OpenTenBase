import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

// Execute page renderers without a browser, then validate generated navigation
// and source references. This does not replace visual/browser verification.
const elements=new Map();
function el(key){if(!elements.has(key))elements.set(key,{innerHTML:'',textContent:'',classList:{toggle(){},remove(){}},addEventListener(){},setAttribute(){},removeAttribute(){}});return elements.get(key);}
const handlers={};
const context=vm.createContext({console,Date,Blob,URL,setTimeout,clearTimeout,setInterval,clearInterval,
 location:{hash:''},window:{addEventListener(type,fn){handlers[type]=fn},scrollTo(){}},
 document:{querySelector:el,querySelectorAll(){return []},addEventListener(){},title:''},
 requestAnimationFrame(){}});
vm.runInContext(fs.readFileSync('data.js','utf8'),context);
vm.runInContext(fs.readFileSync('mechanism.js','utf8'),context);
const data=context.window.OTB_DATA;
let references=0;
for(const page of ['overview','planning','execution','structures','evidence']){
 context.location.hash='#'+page;
 vm.runInContext('render()',context);
 const html=el('#main').innerHTML;
 assert(html.length>1000,page+' is empty');
 assert(!html.includes('undefined'),page+' contains undefined content');
 assert(!html.includes('[object Object]'),page+' contains a serialized object');
 for(const m of html.matchAll(/data-source="([^"]+)" data-line="(\d+)"/g)){
  assert(data.sources[m[1]],'missing source '+m[1]);
  assert(data.sources[m[1]].lines.some(l=>l.n===Number(m[2])),`missing line ${m[1]}:${m[2]}`);
  references++;
 }
 for(const m of html.matchAll(/data-node="([^"]+)"/g))assert(data.nodes.some(n=>n.id===m[1]),'missing node '+m[1]);
 console.log(`PASS ${page}: render and source targets`);
}
for(let i=0;i<data.edges.length;i++){
 vm.runInContext(`S.edge=${i};inspector()`,context);
}
for(const n of data.nodes){
 vm.runInContext(`S.node=${JSON.stringify(n.id)};S.edge=null;inspector()`,context);
}
const result={pages:5,functions:data.nodes.length,relations:data.edges.length,sourceTargets:references,
 status:'passed',visualBrowserQA:'not verified: authentication and local preview access gates'};
fs.writeFileSync('frontend-verification.json',JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify(result));
