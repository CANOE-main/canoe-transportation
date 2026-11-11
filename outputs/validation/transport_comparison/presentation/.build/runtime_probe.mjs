import fs from 'node:fs/promises';
import { Presentation, PresentationFile } from 'file:///C:/Users/rashi/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/@oai/artifact-tool/dist/artifact_tool.mjs';
const out = 'C:/Users/rashi/ESM_databases/canoe-transportation/outputs/validation/transport_comparison/presentation/.build';
const p = Presentation.create({slideSize:{width:1280,height:720}});
const s=p.slides.add();s.background.fill='#FFFFFF';
const t=s.shapes.add({geometry:'textbox',position:{left:60,top:40,width:1150,height:80},fill:'none',line:{fill:'none',width:0}});
t.text='Legacy and new backend';t.text.style={fontFamily:'Calibri',fontSize:44,bold:true,color:'#173F35'};
const chart=s.charts.add('bar',{position:{left:60,top:150,width:690,height:400},categories:['LDV chargers','MHDV chargers'],series:[{name:'Legacy',values:[100,100],fill:'#949E99'},{name:'New backend',values:[98,96],fill:'#1C735C'}],barOptions:{direction:'bar',grouping:'clustered',gapWidth:70},hasLegend:true,legend:{position:'bottom'},dataLabels:{showValue:true,position:'outEnd'},xAxis:{visible:true},yAxis:{visible:true,min:0,max:120}});
const tb=s.tables.add({rows:3,columns:2,left:800,top:160,width:400,height:270,values:[['Class','Driver'],['LDV','Rounded weights'],['MHDV','Rounded weights']]});
tb.cells.block({row:0,column:0,rowCount:3,columnCount:2}).assign({textStyle:{fontFamily:'Calibri',fontSize:21,color:'#173F35'}});
s.speakerNotes.text='Probe. Local artifacts only.';
await (await PresentationFile.exportPptx(p)).save(out+'/probe.pptx');
try { const b=await p.export({slide:s,format:'png',scale:1});await fs.writeFile(out+'/probe.png',new Uint8Array(await b.arrayBuffer()));console.log('RENDER_OK'); } catch(e) {console.log('RENDER_ERROR',e.message);}
console.log('EXPORT_OK');
