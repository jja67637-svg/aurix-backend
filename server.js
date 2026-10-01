import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

const { Pool } = pg;
const PORT=Number(process.env.PORT||8080), HOST=process.env.HOST||'0.0.0.0';
const PUBLIC_ROOT=path.join(path.dirname(fileURLToPath(import.meta.url)),'public');
const REPORT_MAX_AGE=24*60*60*1000, HELP_MAX_AGE=2*60*60*1000, PRESENCE_TTL=35_000;
const TYPES=new Set(['accident','police','camera','roadworks','traffic','vehicle','road','animal','weather','other']);
const HELP_TYPES=new Set(['flat','breakdown','fuel','accident','medical','other']);
const pool = process.env.DATABASE_URL ? new Pool({connectionString:process.env.DATABASE_URL,ssl:{rejectUnauthorized:false},max:5}) : null;
let dbReady=false;

async function initDb(){
  if(!pool)return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS reports(
      id TEXT PRIMARY KEY,type TEXT NOT NULL,variant TEXT,direction TEXT NOT NULL DEFAULT 'same',lat DOUBLE PRECISION NOT NULL,lon DOUBLE PRECISION NOT NULL,
      accuracy DOUBLE PRECISION,heading DOUBLE PRECISION,created_at BIGINT NOT NULL,reported_at TEXT,confirm_count INTEGER NOT NULL DEFAULT 0,reject_count INTEGER NOT NULL DEFAULT 0,
      active BOOLEAN NOT NULL DEFAULT TRUE,text TEXT
    );
    CREATE INDEX IF NOT EXISTS reports_active_created_idx ON reports(active,created_at);
    CREATE TABLE IF NOT EXISTS report_votes(
      report_id TEXT NOT NULL REFERENCES reports(id) ON DELETE CASCADE,user_id TEXT NOT NULL,action TEXT NOT NULL CHECK(action IN ('confirm','reject')),created_at BIGINT NOT NULL,
      PRIMARY KEY(report_id,user_id)
    );
    CREATE TABLE IF NOT EXISTS help_requests(
      id TEXT PRIMARY KEY,type TEXT NOT NULL,lat DOUBLE PRECISION NOT NULL,lon DOUBLE PRECISION NOT NULL,accuracy DOUBLE PRECISION,created_at BIGINT NOT NULL,reported_at TEXT,active BOOLEAN NOT NULL DEFAULT TRUE
    );
    CREATE INDEX IF NOT EXISTS help_active_created_idx ON help_requests(active,created_at);
  `);
  dbReady=true;
}

async function clean(){
  if(!pool||!dbReady)return;
  const now=Date.now();
  await pool.query('DELETE FROM reports WHERE created_at < $1',[now-REPORT_MAX_AGE]);
  await pool.query('DELETE FROM help_requests WHERE created_at < $1',[now-HELP_MAX_AGE]);
}
function num(v){const n=Number(v);return Number.isFinite(n)?n:null}
function validCoord(lat,lon){return lat!==null&&lon!==null&&lat>=-90&&lat<=90&&lon>=-180&&lon<=180}
function distanceKm(a,b){const R=6371.0088,rad=Math.PI/180,dLat=(b.lat-a.lat)*rad,dLon=(b.lon-a.lon)*rad,p1=a.lat*rad,p2=b.lat*rad,h=Math.sin(dLat/2)**2+Math.cos(p1)*Math.cos(p2)*Math.sin(dLon/2)**2;return R*2*Math.atan2(Math.sqrt(Math.min(1,h)),Math.sqrt(Math.max(0,1-h)))}
function sendJson(res,status,obj){const body=JSON.stringify(obj);res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type','Cache-Control':'no-store'});res.end(body)}
function body(req){return new Promise((resolve,reject)=>{let s='';req.on('data',c=>{s+=c;if(s.length>65536)reject(new Error('too large'))});req.on('end',()=>{try{resolve(s?JSON.parse(s):{})}catch(e){reject(e)}});req.on('error',reject)})}
function publicReport(r){return {id:r.id,type:r.type,variant:r.variant,direction:r.direction,lat:r.lat,lon:r.lon,accuracy:r.accuracy,heading:r.heading,createdAt:Number(r.created_at??r.createdAt),reportedAt:r.reported_at??r.reportedAt,confirm:Number(r.confirm_count??r.confirm??0),reject:Number(r.reject_count??r.reject??0),active:r.active!==false,text:r.text??null}}
function sanitizeReport(x){const lat=num(x.lat),lon=num(x.lon),id=String(x.id||'').slice(0,100);if(!id||!validCoord(lat,lon)||!TYPES.has(String(x.type)))return null;return{id,type:String(x.type),variant:x.variant?String(x.variant).slice(0,80):null,direction:x.direction==='opposite'?'opposite':'same',lat,lon,accuracy:num(x.accuracy),heading:num(x.heading),createdAt:num(x.createdAt)||Date.now(),reportedAt:x.reportedAt||new Date().toISOString(),text:x.text?String(x.text).slice(0,160):null}}

const presence=new Map();
function updatePresence(x,ws){const id=String(x.deviceId||'').slice(0,120);if(!id)return null;if(x.online===false){presence.delete(id);return {id,offline:true}}const lat=num(x.lat),lon=num(x.lon),channel=Math.max(1,Math.min(40,Number(x.channel)||19)),range=Math.max(1,Math.min(50,Number(x.range)||10));if(!validCoord(lat,lon))return null;const p={id,lat,lon,channel,range,lastSeen:Date.now(),ws:ws||null,online:true};presence.set(id,p);return p}
function cleanPresence(){const now=Date.now();for(const [id,d] of presence)if(now-d.lastSeen>PRESENCE_TTL)presence.delete(id)}
function peersFor(p){cleanPresence();return [...presence.values()].filter(x=>x.ws&&!x.ws.destroyed&&x.id!==p.id&&x.channel===p.channel&&distanceKm(p,x)<=Math.min(p.range,x.range))}

const server=http.createServer(async(req,res)=>{
  if(req.method==='OPTIONS'){res.writeHead(204,{'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type'});return res.end()}
  const u=new URL(req.url,`http://${req.headers.host||'localhost'}`),p=u.pathname;
  try{
    if(req.method==='GET'&&p==='/health')return sendJson(res,200,{ok:true,service:'aurix-online',database:!!pool&&dbReady,time:new Date().toISOString()});
    if(pool&&!dbReady)await initDb();
    if(req.method==='POST'&&p==='/reports'){
      const r=sanitizeReport(await body(req));if(!r)return sendJson(res,400,{error:'Invalid report'});
      if(!pool)return sendJson(res,503,{error:'DATABASE_URL is required'});
      const q=await pool.query(`INSERT INTO reports(id,type,variant,direction,lat,lon,accuracy,heading,created_at,reported_at,text) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(id) DO UPDATE SET variant=EXCLUDED.variant,direction=EXCLUDED.direction,lat=EXCLUDED.lat,lon=EXCLUDED.lon,accuracy=EXCLUDED.accuracy,heading=EXCLUDED.heading,reported_at=EXCLUDED.reported_at,text=EXCLUDED.text RETURNING *`,[r.id,r.type,r.variant,r.direction,r.lat,r.lon,r.accuracy,r.heading,r.createdAt,r.reportedAt,r.text]);
      return sendJson(res,q.rowCount?200:201,publicReport(q.rows[0]));
    }
    if(req.method==='GET'&&p==='/reports/nearby'){
      const lat=num(u.searchParams.get('lat')),lon=num(u.searchParams.get('lon')),radius=Math.min(100,Math.max(.1,num(u.searchParams.get('radius'))||5));if(!validCoord(lat,lon))return sendJson(res,400,{error:'Invalid location'});if(!pool)return sendJson(res,503,{error:'DATABASE_URL is required'});await clean();const q=await pool.query('SELECT * FROM reports WHERE active=true ORDER BY created_at DESC');const origin={lat,lon};const rows=q.rows.map(r=>({...publicReport(r),distance:distanceKm(origin,r)})).filter(r=>r.distance<=radius).sort((a,b)=>a.distance-b.distance);return sendJson(res,200,rows);
    }
    const voteMatch=p.match(/^\/reports\/([^/]+)\/(confirm|reject)$/);
    if(req.method==='POST'&&voteMatch){const id=voteMatch[1],action=voteMatch[2];if(!pool)return sendJson(res,503,{error:'DATABASE_URL is required'});const x=await body(req),userId=String(x.userId||'').slice(0,120);if(!userId)return sendJson(res,400,{error:'userId required'});const client=await pool.connect();try{await client.query('BEGIN');const rr=await client.query('SELECT * FROM reports WHERE id=$1 FOR UPDATE',[id]);if(!rr.rowCount){await client.query('ROLLBACK');return sendJson(res,404,{error:'Report not active'})}const r=rr.rows[0];if(!r.active){await client.query('ROLLBACK');return sendJson(res,404,{error:'Report not active'})}const v=await client.query('INSERT INTO report_votes(report_id,user_id,action,created_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING RETURNING report_id',[id,userId,action,Date.now()]);if(!v.rowCount){await client.query('ROLLBACK');return sendJson(res,409,{error:'Already voted',action,report:publicReport(r)})}if(action==='confirm')await client.query('UPDATE reports SET confirm_count=confirm_count+1 WHERE id=$1',[id]);else await client.query('UPDATE reports SET reject_count=reject_count+1,active=CASE WHEN reject_count+1>=5 THEN false ELSE active END WHERE id=$1',[id]);const out=await client.query('SELECT * FROM reports WHERE id=$1',[id]);await client.query('COMMIT');return sendJson(res,200,publicReport(out.rows[0]));}catch(e){await client.query('ROLLBACK');throw e}finally{client.release()}}
    if(req.method==='POST'&&p==='/help'){const x=await body(req),lat=num(x.lat),lon=num(x.lon),type=String(x.helpType||'').replace(/^help_/,'');if(!validCoord(lat,lon)||!HELP_TYPES.has(type))return sendJson(res,400,{error:'Invalid help request'});if(!pool)return sendJson(res,503,{error:'DATABASE_URL is required'});const id=String(x.id||'').slice(0,100)||`h_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`,createdAt=num(x.createdAt)||Date.now(),reportedAt=x.reportedAt||new Date(createdAt).toISOString();const q=await pool.query('INSERT INTO help_requests(id,type,lat,lon,accuracy,created_at,reported_at) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *',[id,type,lat,lon,num(x.accuracy),createdAt,reportedAt]);return sendJson(res,201,{id:q.rows[0].id,type:`help_${q.rows[0].type}`,helpType:q.rows[0].type,lat:q.rows[0].lat,lon:q.rows[0].lon,accuracy:q.rows[0].accuracy,createdAt:Number(q.rows[0].created_at),reportedAt:q.rows[0].reported_at,active:true});}
    if(req.method==='GET'&&p==='/help/nearby'){const lat=num(u.searchParams.get('lat')),lon=num(u.searchParams.get('lon')),radius=Math.min(50,Math.max(.1,num(u.searchParams.get('radius'))||10));if(!validCoord(lat,lon))return sendJson(res,400,{error:'Invalid location'});if(!pool)return sendJson(res,503,{error:'DATABASE_URL is required'});await clean();const q=await pool.query('SELECT * FROM help_requests WHERE active=true ORDER BY created_at DESC');const origin={lat,lon},rows=q.rows.map(h=>({id:h.id,type:`help_${h.type}`,helpType:h.type,lat:h.lat,lon:h.lon,accuracy:h.accuracy,createdAt:Number(h.created_at),reportedAt:h.reported_at,active:h.active,distance:distanceKm(origin,h)})).filter(h=>h.distance<=radius).sort((a,b)=>a.distance-b.distance);return sendJson(res,200,rows)}
    if(req.method==='GET'&&p==='/speed-cameras/nearby')return sendJson(res,200,[]);
    if(req.method==='POST'&&p==='/presence'){const x=await body(req),me=updatePresence(x,null);if(!me)return sendJson(res,400,{error:'Invalid presence'});if(me.offline)return sendJson(res,200,{count:0,online:false});const count=peersFor(me).length;return sendJson(res,200,{count,online:true})}
    if(req.method==='GET'&&p==='/cb/presence'){const lat=num(u.searchParams.get('lat')),lon=num(u.searchParams.get('lon')),channel=Math.max(1,Math.min(40,Number(u.searchParams.get('channel'))||19)),range=Math.max(1,Math.min(50,Number(u.searchParams.get('range'))||10)),self=String(u.searchParams.get('deviceId')||'');if(!validCoord(lat,lon))return sendJson(res,200,{count:0});cleanPresence();const o={lat,lon},count=[...presence.values()].filter(v=>v.online&&v.id!==self&&v.channel===channel&&distanceKm(o,v)<=Math.min(range,v.range)).length;return sendJson(res,200,{count})}
    if(req.method==='GET'){let rel=p==='/'?'index.html':p.replace(/^\//,'');if(rel.includes('..'))return sendJson(res,400,{error:'Bad path'});const file=path.join(PUBLIC_ROOT,rel);try{const data=fs.readFileSync(file);const types={'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.svg':'image/svg+xml','.png':'image/png','.ico':'image/x-icon'};res.writeHead(200,{'Content-Type':types[path.extname(file)]||'application/octet-stream','Cache-Control':'no-cache'});return res.end(data)}catch{}}
    return sendJson(res,404,{error:'Not found'});
  }catch(e){console.error(e);return sendJson(res,500,{error:'Server error'})}
});

const sockets=new Set();
function wsFrame(text){const payload=Buffer.from(text);let h;if(payload.length<126){h=Buffer.alloc(2);h[1]=payload.length}else if(payload.length<65536){h=Buffer.alloc(4);h[1]=126;h.writeUInt16BE(payload.length,2)}else{h=Buffer.alloc(10);h[1]=127;h.writeBigUInt64BE(BigInt(payload.length),2)}h[0]=0x81;return Buffer.concat([h,payload])}
function wsSend(sock,obj){try{if(!sock.destroyed)sock.write(wsFrame(JSON.stringify(obj)))}catch{}}
function closeWs(sock){try{sock.end(Buffer.from([0x88,0x00]))}catch{}}
function attachWs(sock){sock.buffer=Buffer.alloc(0);sock.on('data',chunk=>{sock.buffer=Buffer.concat([sock.buffer,chunk]);parseWs(sock)});sock.on('close',()=>cleanupWs(sock));sock.on('error',()=>cleanupWs(sock));sockets.add(sock)}
function parseWs(sock){while(sock.buffer.length>=2){const b0=sock.buffer[0],b1=sock.buffer[1],opcode=b0&15,masked=!!(b1&128);let len=b1&127,off=2;if(len===126){if(sock.buffer.length<4)return;len=sock.buffer.readUInt16BE(2);off=4}else if(len===127){if(sock.buffer.length<10)return;const n=sock.buffer.readBigUInt64BE(2);if(n>BigInt(10_000_000))return closeWs(sock);len=Number(n);off=10}const need=off+(masked?4:0)+len;if(sock.buffer.length<need)return;let mask;if(masked){mask=sock.buffer.subarray(off,off+4);off+=4}let data=sock.buffer.subarray(off,off+len);sock.buffer=sock.buffer.subarray(need);if(masked){data=Buffer.from(data);for(let i=0;i<data.length;i++)data[i]^=mask[i%4]}if(opcode===8){closeWs(sock);return}if(opcode===9){try{sock.write(Buffer.from([0x8a,0]))}catch{}continue}if(opcode!==1)continue;handleWsMessage(sock,data.toString())}}
function handleWsMessage(sock,raw){let m;try{m=JSON.parse(raw)}catch{return}if(m.type==='presence'){const previous=sock.aurixId,me=updatePresence(m,sock);if(!me)return;if(me.offline){if(previous)presence.delete(previous);sock.aurixId=null;for(const d of presence.values())if(d.ws)wsSend(d.ws,{type:'peer_leave',peerId:me.id});return;}sock.aurixId=me.id;if(previous&&previous!==me.id)presence.delete(previous);const peers=peersFor(me);wsSend(sock,{type:'peers',peers:peers.map(x=>x.id)});for(const other of peers){wsSend(other.ws,{type:'peer_join',peerId:me.id});wsSend(sock,{type:'peer_join',peerId:other.id})}return}if(m.type==='signal'&&sock.aurixId){const me=presence.get(sock.aurixId),target=presence.get(String(m.to||''));if(!me||!target?.ws)return;if(target.channel!==me.channel||distanceKm(me,target)>Math.min(me.range,target.range))return;wsSend(target.ws,{type:'signal',from:me.id,data:m.data})}}
function cleanupWs(sock){if(!sockets.has(sock))return;sockets.delete(sock);const id=sock.aurixId;if(id){presence.delete(id);for(const d of presence.values())if(d.ws)wsSend(d.ws,{type:'peer_leave',peerId:id})}}
server.on('upgrade',(req,socket)=>{const u=new URL(req.url,`http://${req.headers.host||'localhost'}`);if(u.pathname!=='/ws'){socket.destroy();return}const key=req.headers['sec-websocket-key'];if(!key){socket.destroy();return}const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');attachWs(socket)});
setInterval(()=>{cleanPresence();clean().catch(()=>{})},15000).unref();
initDb().then(()=>server.listen(PORT,HOST,()=>console.log(`AURIX ONLINE listening on ${HOST}:${PORT}`))).catch(e=>{console.error('Database init failed',e);process.exit(1)});


