import fs from 'node:fs/promises';
import path from 'node:path';
import { Presentation, PresentationFile } from 'file:///C:/Users/rashi/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/@oai/artifact-tool/dist/artifact_tool.mjs';
import { applyPresentationChartFont, finalizePresentation } from 'file:///C:/Users/rashi/.codex/plugins/cache/openai-primary-runtime/presentations/26.1009.11318/skills/presentations/container_tools/artifact_tool_utils.mjs';

const ROOT='C:/Users/rashi/ESM_databases/canoe-transportation';
const WORK=ROOT+'/outputs/validation/transport_comparison/presentation';
const BUILD=WORK+'/.build';
const SKILL='C:/Users/rashi/.codex/plugins/cache/openai-primary-runtime/presentations/26.1009.11318/skills/presentations';
const PY='C:/Users/rashi/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe';
const evidence=JSON.parse(await fs.readFile(BUILD+'/chart_evidence.json','utf8'));
const byId=Object.fromEntries(evidence.slides.map(s=>[s.id,s]));
const C={ink:'#173F35',green:'#17795E',gray:'#919B97',light:'#F0F5F2',line:'#D8E2DC',text:'#263C34',muted:'#5A6D63',amber:'#966A20',white:'#FFFFFF'};
const FONT='Arial';
const p=Presentation.create({slideSize:{width:1280,height:720}});
const pages=[];
const exports=[];
const pct=(v,d=1)=>(v>=0?'+':'')+v.toFixed(d)+'%';
const fmt=(v,d=2)=>v.toLocaleString('en-CA',{minimumFractionDigits:d,maximumFractionDigits:d});

function text(s,value,x,y,w,h,size=24,opts={}){
  const t=s.shapes.add({geometry:'textbox',name:opts.name||value.slice(0,48),position:{left:x,top:y,width:w,height:h},fill:'none',line:{fill:'none',width:0}});
  t.text=value;t.text.style={typeface:FONT,fontSize:size,color:opts.color||C.text,bold:!!opts.bold,verticalAlignment:'top',autoFit:'none',insets:0,...opts};return t;
}
function makeSlide(parameter,title,subtitle,ids=[],foot='Ontario. Selected examples, not fleet averages.'){
  const s=p.slides.add();s.background.fill=C.white;
  text(s,parameter,54,26,1170,25,17,{color:C.muted});
  text(s,title,54,66,1170,62,40,{bold:true,color:C.ink});
  text(s,subtitle,54,131,1170,55,22,{color:C.muted});
  text(s,foot,54,678,1130,24,15,{color:C.muted});
  text(s,String(p.slides.items.length).padStart(2,'0'),1190,678,40,24,15,{color:C.muted,alignment:'right'});
  const notes=['Ontario. Numeric source pair:',`Legacy: ${evidence.sources.legacy.path}`,`Legacy SHA256: ${evidence.sources.legacy.sha256}`,`New backend: ${evidence.sources.candidate.path}`,`New SHA256: ${evidence.sources.candidate.sha256}`,'New database build: 2026-10-13, legacy_comparison scenario.','Row-level evidence: outputs/validation/transport_comparison/rows.csv',`rows.csv SHA256: ${evidence.rows_csv_sha256}`,'Explanations: user-supplied observations, 2026-10-13. They are reported drivers, not a quantitative decomposition of each gap.'];
  for(const id of ids){const e=byId[id];notes.push(`\n${e.parameter}: ${e.driver_text}`,`Comparison: ${e.bar_method}`,`Interpretation: ${e.caveats.join(' ')}`);}
  s.speakerNotes.text=notes.join('\n');
  pages.push({slide:p.slides.items.length,parameter,title,ids});return s;
}
function addNotes(s,items){s.speakerNotes.text += '\n\nPlotted records and source keys:\n'+JSON.stringify(items,null,2);}
function table(s,values,box,widths,font=21){
 const t=s.tables.add({rows:values.length,columns:values[0].length,left:box.x,top:box.y,width:box.w,height:box.h,columnWidths:widths,values});
 t.borders.assign({style:'solid',fill:C.line,width:0.7});
 t.cells.block({row:0,column:0,rowCount:values.length,columnCount:values[0].length}).assign({textStyle:{typeface:FONT,fontSize:font,color:C.text},margins:{left:12,right:12,top:9,bottom:8},anchor:'center'});
 for(let r=0;r<values.length;r++)for(let c=0;c<values[0].length;c++){
  t.getCell(r,c).fill=r===0?C.light:C.white;
  t.getCell(r,c).text.style={typeface:FONT,fontSize:r===0?font-1:font,bold:r===0,color:r===0?C.ink:C.text,verticalAlignment:'middle',insets:{left:12,right:12,top:8,bottom:8}};
 }
 t.rows[0].height=42;
 return t;
}
function bar(s,items,box,{kind='percent',title='',max,min=0,majorUnit,decimals=1,legend=true,series,labels,axisTitle}={}){
 const rs=[...items].reverse();
 let chartSeries=series?series.map(q=>({...q,values:[...q.values].reverse()})):
  kind==='percent'?[{name:'Change from legacy',values:rs.map(r=>(r.candidate-r.legacy)/Math.abs(r.legacy)),fill:C.green}]:
  [{name:'Legacy',values:rs.map(r=>r.legacy),fill:C.gray},{name:'New backend',values:rs.map(r=>r.candidate),fill:C.green}];
 // Excel preserves 15 significant digits. Ten retain far more than the displayed
 // precision while the notes and evidence JSON preserve complete source values.
 chartSeries=chartSeries.map(q=>({...q,values:q.values.map(v=>v===null?null:Number(v.toPrecision(10)))}));
 const isPct=kind==='percent';
 const all=chartSeries.flatMap(q=>q.values).filter(v=>v!==null);
 if(min===0&&Math.min(...all)<0)min=Math.floor(Math.min(...all)*1.3*10)/10;
 max??=Math.max(...all)>0?Math.max(...all)*1.23:0;
 if(isPct&&max===0)max=0.025;
 const format=isPct?`+0.${'0'.repeat(decimals)}%;-0.${'0'.repeat(decimals)}%;0.${'0'.repeat(decimals)}%`:`#,##0.${'0'.repeat(decimals)}`;
 const chart=s.charts.add('bar',{
  position:{left:box.x,top:box.y,width:box.w,height:box.h},title,titlePlacement:title?'aboveChart':'none',titleTextStyle:{fontSize:20,fill:C.muted},
  categories:labels?[...labels].reverse():rs.map(r=>r.label),series:chartSeries,
  barOptions:{direction:'bar',grouping:'clustered',gapWidth:kind==='percent'?70:52,overlap:0},
  hasLegend:kind!=='percent'&&legend,legend:{position:'bottom',overlay:false,textStyle:{fontSize:18,fill:C.muted}},
  chartFill:C.white,plotAreaFill:C.white,chartLine:{fill:'none',width:0},plotAreaLine:{fill:'none',width:0},
  xAxis:{visible:true,textStyle:{fontSize:20,fill:C.text},line:{fill:'none',width:0},tickLabelPosition:'low',majorGridlines:null},
  yAxis:{visible:true,min,max,majorUnit,numberFormatCode:isPct?(Math.max(Math.abs(min),Math.abs(max))<0.1?'0.0%':'0%'):`0.${'0'.repeat(Math.min(decimals,2))}`,textStyle:{fontSize:16,fill:C.muted},line:{fill:C.line,width:0.5},majorGridlines:{fill:C.line,width:0.5},title:axisTitle?{text:axisTitle,textStyle:{fontSize:17,fill:C.muted}}:undefined},
  dataLabels:{showValue:true,position:'outEnd',numberFormatCode:format,textStyle:{fontSize:20,fill:C.text,bold:true}},
 });
 applyPresentationChartFont(chart,{fontFamily:FONT});
 exports.push({slide:p.slides.items.length,title,kind,categories:rs.map(r=>r.label),series:chartSeries.map(q=>({name:q.name,values:q.values})),records:items});
 return chart;
}

// Cover also states how the representative charts should be read.
{
 const s=p.slides.add();s.background.fill=C.ink;
 text(s,'CANOE TRANSPORTATION',64,54,1140,35,21,{color:'#B6CEC2'});
 text(s,'Legacy and new backend\nObserved differences',64,138,1140,150,55,{color:C.white,bold:true});
 text(s,'Ontario comparison',64,328,1140,50,32,{color:'#CEE2D8'});
 text(s,'Legacy: canoe_transport_baseline.sqlite\nNew: legacy_reproduction.sqlite, generated 13 October 2026',64,405,1140,83,23,{color:C.white});
 text(s,'Seven parameters, grouped by affected technology and difference driver.\nBars show selected class examples. Percentage changes use the legacy value as the base.\nMost future examples use 2025 model values. Each slide states its time slice.',64,554,1140,95,22,{color:'#CEE2D8'});
 s.speakerNotes.text='Scope: cost_fixed, cost_invest, cost_variable, demand, efficiency, existing_capacity, limit_annual_capacity_factor. User-approved geography is Ontario. Explanations are limited to user observations, not a quantitative causal decomposition.\n'+JSON.stringify({database_pair:evidence.sources,rows_csv_sha256:evidence.rows_csv_sha256},null,2);
 pages.push({slide:1,parameter:'cover',title:'Legacy and new backend: Observed differences'});
}

{
 const e=byId.fixed_chargers;
 const s=makeSlide('cost_fixed','Small differences in charger fixed costs','2025 vintage and period. Both charger classes differ by less than 1% in this example.',['fixed_chargers'],'Ontario. Matching legacy class rates bridge to the new shared charger classes.');
 bar(s,e.items,{x:55,y:202,w:720,h:280},{title:'Change from legacy',max:0.012,decimals:2});
 text(s,'Rounded aggregation weights',818,225,380,90,31,{bold:true,color:C.ink});
 text(s,'LDV and MHDV chargers share the same reported difference driver.',818,335,380,110,25);
 table(s,[['Class','Legacy','New backend','Cost basis'],...e.items.map(r=>[r.label,fmt(r.legacy,3),fmt(r.candidate,3),'Million 2020 CAD / GW'])],{x:54,y:522,w:1172,h:133},[250,190,210,522],22);addNotes(s,e.items);
}
{
 const e=byId.investment_offroad;
 const s=makeSlide('cost_invest','Investment cost for heavy off-road modes','2025 vintage. Differences reflect capital costs per unit of transport service.',['investment_offroad']);
 bar(s,e.items,{x:54,y:191,w:1172,h:298},{title:'Change from legacy',max:8});
 table(s,[['Affected classes','Difference driver'],['All heavy off-road','CIMS replaces OEO capital costs, normalized to demand-service units.'],['Aircraft','Service output comes from FAA operational data.']],{x:54,y:518,w:1172,h:145},[265,907],22);addNotes(s,e.items);
}
{
 const e=byId.investment_road;
 const s=makeSlide('cost_invest','Investment cost for road vehicles','2025 vintage. Truck examples show larger differences than cars and light trucks.',['investment_road']);
 bar(s,e.items,{x:54,y:190,w:687,h:450},{title:'Change from legacy',min:-0.4,max:0,majorUnit:0.1,labels:['Car, gasoline','Light truck, gasoline','Medium truck, diesel','Medium truck, gasoline','Heavy truck, diesel','Heavy truck, BEV']});
 table(s,[['Class','Difference driver'],['Road vehicles','NLR ATB retail price / 1.5 should preserve a similar cost basis. Aggregation weights are the expected main driver.'],['Heavy trucks','Hypothesis: the changed regional / long-haul mix explains the larger gap.'],['Gasoline medium trucks','ATB lacks the gasoline input included in legacy Islam et al. (2023) results.']],{x:775,y:205,w:451,h:446},[133,318],20);addNotes(s,e.items);
}
{
 const e=byId.variable_offroad;
 const s=makeSlide('cost_variable','Operating cost for heavy off-road modes','2025 vintage and period. Marine uses its own scale so the other classes remain visible.',['variable_offroad'],'Ontario. The listed methods do not quantify every part of the observed cost gap.');
 bar(s,[e.items[0]],{x:54,y:196,w:525,h:287},{kind:'value',title:'Million 2020 CAD / billion tonne-km',decimals:3,max:10,labels:['Marine diesel oil']});
 bar(s,e.items.slice(1),{x:622,y:196,w:604,h:287},{title:'Other modes: change from legacy',max:5.5,labels:['Rail freight','Air freight','Passenger rail','Passenger air']});
 table(s,[['Affected classes','Difference driver'],['Marine and rail','Revised capital costs feed into OEO capital-to-operating cost ratios.'],['Aircraft','FAA operational data supplies operating costs.']],{x:54,y:516,w:1172,h:145},[265,907],22);addNotes(s,e.items);
}
{
 const e=byId.variable_road;
 const s=makeSlide('cost_variable','Road operating costs now use formulas','2025 vintage and period. The direction of change varies across vehicle classes.',['variable_road']);
 bar(s,e.items,{x:54,y:192,w:1172,h:321},{title:'Change from legacy',min:-1.2,max:1.2,majorUnit:0.2,labels:['Car, gasoline','Light truck, gasoline','School bus, diesel','Transit bus, diesel','Medium truck, diesel','Heavy truck, diesel']});
 table(s,[['Affected classes','Legacy basis','New backend basis'],['Road vehicles','Hard-coded costs fetched directly from the BEAN model.','Costs calculated with formulas.']],{x:54,y:537,w:1172,h:123},[254,458,460],23);addNotes(s,e.items);
}
{
 const a=byId.demand_marine,b=byId.demand_other;
 const selected=[a.items[0],...['Intercity bus','Cars','Passenger light trucks','Heavy trucks','Passenger air'].map(l=>b.items.find(r=>r.label===l))];
 const s=makeSlide('demand','Demand changes with the edition and base year','2025 model period. Marine is shown alongside selected examples of the other modes.',['demand_marine','demand_other'],'Ontario. Model-period demand, not an observed 2025 measurement.');
 bar(s,selected,{x:54,y:193,w:1172,h:317},{title:'Change from legacy',min:-1.0,max:0.2});
 table(s,[['Affected modes','Difference driver'],['Marine freight','The CEUD edition change substantially alters marine energy use.'],['All other modes','Future demand is indexed to 2023 demand instead of 2019.']],{x:54,y:524,w:1172,h:138},[266,906],22);addNotes(s,selected);
}
{
 const e=byId.efficiency_bus_marine,rs=e.items.slice(0,4);
 const s=makeSlide('efficiency','Existing bus efficiency uses CEUD backcasting','2020 vintage. Values show transport service per unit of energy.',['efficiency_bus_marine']);
 bar(s,rs,{x:54,y:196,w:715,h:438},{kind:'value',title:'Billion passenger-km / PJ',decimals:2,max:4.5,labels:['School bus, diesel','School bus, gasoline','Transit bus, diesel','Transit bus, CNG']});
 table(s,[['Version','Existing-stock method'],['Legacy','GREET gasoline-to-diesel and CNG-to-diesel scaling, with improvements indexed to ATB.'],['New backend','ATB values backcast using CEUD energy intensities.']],{x:798,y:224,w:428,h:354},[130,298],22);addNotes(s,rs);
}
{
 const e=byId.efficiency_bus_marine,rs=e.items.slice(4);
 const s=makeSlide('efficiency','Marine efficiency uses CEUD and REGEN','2025 vintage. Marine freight efficiency is lower in both fuel examples.',['efficiency_bus_marine']);
 bar(s,rs,{x:54,y:201,w:703,h:382},{kind:'value',title:'Billion tonne-km / PJ',decimals:2,max:8.3,labels:['Heavy fuel oil','Marine diesel oil']});
 table(s,[['Version','Efficiency basis'],['Legacy','Values from a regional report.'],['New backend','CEUD energy intensities, with improvement indexed to REGEN assumptions.']],{x:798,y:226,w:428,h:335},[127,301],23);
 text(s,`Heavy fuel oil: ${pct(rs[0].relative_percent)}     Marine diesel oil: ${pct(rs[1].relative_percent)}`,54,615,1172,36,24,{color:C.ink,bold:true});addNotes(s,rs);
}
{
 const e=byId.efficiency_merged_ev;
 const s=makeSlide('efficiency','Existing BEV and PHEV archetypes merge range evidence','2020 vintage. Each legacy range-specific representative maps to its new merged class.',['efficiency_merged_ev'],'Ontario. Explicit class mapping across representations, not identical technology keys.');
 bar(s,e.items,{x:54,y:196,w:1172,h:306},{kind:'value',title:'Billion passenger-km / PJ',decimals:2,max:3.0,labels:['Car, BEV','Passenger light truck, BEV','Car, PHEV','Passenger light truck, PHEV']});
 table(s,[['Affected archetypes','Legacy basis','New backend basis'],['Existing BEV and PHEV','A fixed range class represents each archetype.','Merged archetypes aggregate range evidence using OMEGA vehicle-sales evidence.']],{x:54,y:532,w:1172,h:131},[268,375,529],22);addNotes(s,e.items);
}
{
 const e=byId.capacity_marine_road;
 const rs=e.items;
 const s=makeSlide('existing_capacity','Existing capacity changes with the source baseline','Each version’s baseline inventory, summed across its stored existing cohorts.',['capacity_marine_road'],'Ontario. Road inventory uses 2021 versus 2023 baselines; each marine bar retains its fuel class.');
 bar(s,rs,{x:54,y:193,w:1172,h:318},{title:'Change from legacy',min:-1.0,max:0.4,labels:['Marine freight, HFO','Marine freight, MDO','Cars, all powertrains','Passenger light trucks, all powertrains','Medium trucks, all powertrains','Heavy trucks, all powertrains']});
 table(s,[['Affected classes','Difference driver'],['Marine freight','Fuel-consumption differences between CEUD editions.'],['Road vehicles','The stock baseline shifts from 2021 to 2023.']],{x:54,y:526,w:1172,h:137},[267,905],23);addNotes(s,rs);
}
{
 const e=byId.capacity_chargers;
 const s=makeSlide('existing_capacity','Charger capacity follows ports and EV-to-port ratios','Each version’s baseline inventory. Legacy charger cohorts are summed within each class.',['capacity_chargers'],'Ontario. Legacy MHDV inventory contains HDV rows only; no MDV charger-stock rows are present.');
 bar(s,e.items,{x:54,y:200,w:717,h:392},{kind:'value',title:'Installed charger capacity (GW)',decimals:3,max:1.12,series:[{name:'Legacy inventory',values:e.items.map(r=>r.legacy),fill:C.gray},{name:'New 2023 inventory',values:e.items.map(r=>r.candidate),fill:C.green}]});
 table(s,[['Version','Capacity calibration'],['Legacy','Energy-based calibration.'],['New backend','Public port counts and EV-to-port ratios.']],{x:798,y:218,w:428,h:333},[136,292],23);addNotes(s,e.items);
}
{
 const e=byId.annual_cf;
 const rs=['Cars','Medium trucks','Heavy trucks','Intercity buses'].map(l=>e.items.find(r=>r.label===l));
 const s=makeSlide('limit_annual_capacity_factor','Road utilization reflects a revised CEUD averaging window','Latest five eligible activity-to-stock periods, excluding 2020–2021.',['annual_cf'],'Ontario. 2025 vintage at its installation period. Legacy bounds and the new equality value have different roles.');
 bar(s,rs,{x:54,y:203,w:690,h:377},{kind:'value',title:'Annual capacity factor (%)',decimals:2,max:30,series:[{name:'Legacy upper limit',values:rs.map(r=>100*r.legacy_upper),fill:C.gray},{name:'New fixed value',values:rs.map(r=>100*r.candidate),fill:C.green}]});
 table(s,[['Class','Legacy range (%)','New (%)'],...rs.map(r=>[r.label,`${fmt(100*r.legacy_lower,3)}–${fmt(100*r.legacy_upper,3)}`,fmt(100*r.candidate,3)])],{x:778,y:226,w:448,h:317},[134,191,123],19);
 text(s,'Changed CEUD editions change the baseline activity-to-stock averages.',54,615,1172,37,25,{color:C.ink,bold:true});addNotes(s,rs);
}

await fs.mkdir(BUILD+'/renders',{recursive:true});
await fs.writeFile(BUILD+'/slide_manifest.json',JSON.stringify({pages,charts:exports},null,2));
await fs.writeFile(BUILD+'/presentation.json',JSON.stringify(p.toProto()));
const draft=BUILD+'/draft.pptx';
await (await PresentationFile.exportPptx(p)).save(draft);
for(const [i,s] of p.slides.items.entries()){
 const image=await p.export({slide:s,format:'png',scale:1.5});
 await fs.writeFile(BUILD+`/renders/slide-${String(i+1).padStart(2,'0')}.png`,new Uint8Array(await image.arrayBuffer()));
}
const revision=process.argv[2]||'v1';
const finalPath=WORK+'/output/canoe_legacy_backend_differences_ontario_'+revision+'.pptx';
const result=await finalizePresentation({workspaceDir:WORK,candidatePath:draft,finalPath,pythonExecutable:PY,integrityValidatorPath:path.join(SKILL,'container_tools/inspect_presentation_package_integrity.py'),layoutValidatorPath:path.join(SKILL,'container_tools/inspect_presentation_layout_geometry.py'),layoutArgs:['--expected-slide-size-emu','12192000,6858000','--validate-heading-fit',...pages.filter(x=>x.parameter!=='cover').flatMap(x=>['--require-native-table-slide',String(x.slide)])],explicitTotalSlideCount:13,requiredNativeTableOwnerSlides:pages.filter(x=>x.parameter!=='cover').map(x=>x.slide),requiredNativeChartOwnerSlides:pages.filter(x=>x.parameter!=='cover').map(x=>x.slide),materializeLiteralChartWorkbooks:true,fontPolicy:{basis:'design',families:[FONT]},verifyArtifactToolImport:true,receiptPath:BUILD+'/validation_'+revision+'.json'});
console.log(JSON.stringify({finalPath,slides:p.slides.items.length,validated:!!result}));
