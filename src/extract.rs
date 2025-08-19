use thirtyfour::prelude::WebDriver;
use serde_json::Value;
use anyhow::Result;

pub async fn listing_old_top_day(drv: &WebDriver) -> Result<Vec<(String, Option<String>, Option<i64>)>> {
    let js = r#"
    const out=[];
    const els = document.querySelectorAll('div#siteTable div.thing.link');
    els.forEach(el=>{
      const fn = el.getAttribute('data-fullname') || '';
      const id = fn.startsWith('t3_') ? fn.slice(3) : null;
      let href = null;
      const c = el.querySelector('a.comments');
      if (c) href = c.href;
      if (id) out.push([id, href, null]);
    });
    return out;
    "#;
    let v: Value = drv.execute(js, vec![]).await?.convert()?;
    let arr = v.as_array().cloned().unwrap_or_default();
    Ok(arr.into_iter().filter_map(|t| {
        let id = t.get(0)?.as_str()?.to_string();
        let href = t.get(1).and_then(|x| x.as_str()).map(|s| s.to_string());
        let ts = t.get(2).and_then(|x| x.as_i64());
        Some((id, href, ts))
    }).collect())
}

pub async fn post_old_page(drv: &WebDriver) -> Result<Value> {
    let js = r#"
        function text(el){ return el ? (el.textContent||'').trim() : null; }
        function digits(s){ if(!s) return null; const m=(s.match(/\d[\d,]*/)||[])[0]; return m?parseInt(m.replace(/,/g,'')):null; }
        const res={title:null,author:null,score:null,created_utc:null,selftext:null,num_comments:null,images:[],comments:[]};
        const main=document.querySelector('div#siteTable div.thing.link');
        if(main){
            res.title=text(main.querySelector('a.title'));
            res.author=text(main.querySelector('a.author'));
            const sc=main.querySelector('div.score'); res.score=digits(sc?(sc.getAttribute('title')||sc.textContent):null);
            const tm=main.querySelector('time'); if(tm&&tm.dateTime){res.created_utc=Math.floor(Date.parse(tm.dateTime)/1000);}
            res.num_comments=digits(text(main.querySelector('a.comments')));
            res.selftext=text(main.querySelector('div.expando div.usertext div.usertext-body'));
            const imgset=new Set();
            ['div.expando img','a.thumbnail img','div.expando a[rel="nofollow"] img'].forEach(sel=>{
                main.querySelectorAll(sel).forEach(img=>{const u=img.getAttribute('src')||''; if(u&&!u.startsWith('data:')) imgset.add(u);});
            });
            main.querySelectorAll('div.expando a').forEach(a=>{
                const h=a.getAttribute('href')||'';
                if(/\.(jpg|jpeg|png|gif)$/i.test(h)) imgset.add(h);
            });
            if(imgset.size===0){
                document.querySelectorAll('div.content img').forEach(img=>{
                    const u=img.getAttribute('src')||'';
                    if(u&&!u.startsWith('data:')&&!/emoji/i.test(u)) imgset.add(u);
                });
            }
            res.images=Array.from(imgset);
        }
        document.querySelectorAll('div.sitetable.nestedlisting div.thing.comment').forEach(c=>{
            const fn=c.getAttribute('data-fullname')||''; const id=fn.startsWith('t1_')?fn.slice(3):null; if(!id) return;
            const parent=c.getAttribute('data-parent')||null;
            const author=text(c.querySelector('a.author'));
            const s1=text(c.querySelector('span.score.unvoted'))||text(c.querySelector('span.score')); const score=digits(s1);
            let created_utc=null; const tm=c.querySelector('time'); if(tm&&tm.dateTime){ created_utc=Math.floor(Date.parse(tm.dateTime)/1000); }
            const body=text(c.querySelector('div.entry div.usertext-body'));
            res.comments.push({id, parent_fullname:parent, author, body, score, created_utc});
        });
        return res;
    "#;
    let v: Value = drv.execute(js, vec![]).await?.convert()?;
    Ok(v)
}
