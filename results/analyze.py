import csv, statistics as st, math, random, sys
from scipy import stats
random.seed(1)
def pairs(path, phase=None):
    r=list(csv.DictReader(open(path)))
    out={}
    for ph in sorted({x.get('phase','record') for x in r}):
        u={int(x['run']):int(x['msec']) for x in r if x['mode']=='unsafe' and x.get('phase','record')==ph}
        j={int(x['run']):int(x['msec']) for x in r if x['mode']=='jni' and x.get('phase','record')==ph}
        k=sorted(u); out[ph]=([u[i] for i in k],[j[i] for i in k])
    return out
def report(name,U,J):
    d=[b-a for a,b in zip(U,J)]; m=st.mean(U); se=st.stdev(d)/math.sqrt(len(d)); t=stats.t.ppf(.975,len(d)-1)
    def boot(f):
        o=sorted(100*(f([random.choice(J) for _ in J])/f([random.choice(U) for _ in U])-1) for _ in range(10000)); return o[250],o[9750]
    lo,hi=boot(st.median)
    print(f'== {name}  n={len(U)} pairs')
    print(f'  unsafe mean {m:.0f} sd {st.stdev(U):.0f} median {st.median(U):.0f} | jni mean {st.mean(J):.0f} sd {st.stdev(J):.0f} median {st.median(J):.0f}')
    print(f'  mean diff {100*st.mean(d)/m:+.2f}% 95% CI [{100*(st.mean(d)-t*se)/m:+.2f}, {100*(st.mean(d)+t*se)/m:+.2f}]')
    print(f'  median diff {100*(st.median(J)/st.median(U)-1):+.2f}% 95% bootstrap CI [{lo:+.1f}, {hi:+.1f}]')
    print(f'  Welch p={stats.ttest_ind(J,U,equal_var=False).pvalue:.3f} paired t p={stats.ttest_rel(J,U).pvalue:.3f} Mann-Whitney p={stats.mannwhitneyu(J,U).pvalue:.3f} Wilcoxon p={stats.wilcoxon(J,U).pvalue:.3f}')
for f in ('avrora_record.csv','batik_rr.csv'):
    for ph,(U,J) in pairs(f).items(): report(f'{f} {ph}',U,J)
