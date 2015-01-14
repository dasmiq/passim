## There are several dummy targets. Most interesting are:
## make align
## make clinfo

## This is the subdirectory where your input files live in "input".
## Edit this if you have only one collection.
## Otherwise, pass it as an argument to make, e.g.:
## make COLL=bills align
COLL=coll

LC_ALL=POSIX
export LC_ALL

PATH:=$(shell pwd):${PATH}
export PATH

galagoIndex=galago
galagoMode=

ifneq ($(shell which bsub 2>/dev/null),)
q=lsf
else ifneq ($(shell which qsub 2>/dev/null),)
q=sge
else
q=
endif

ifeq (${q},lsf)
q=lsf
sub=bsub -n 1 -q ser-par-10g
QRUN=${sub} -K -e $@.err
galagoIndex=lsf-galago
else ifeq (${q},sge)
q=sge
sub=qsub -b y -j y -cwd -V -S /bin/bash
QRUN=${sub} -sync y -o $@.err
galagoMode=--mode=drmaa
else
q=
sub=
QRUN=/bin/bash -o pipefail -c
endif

## Number of concurrent jobs (could be smaller than total number of tasks GDISTRIB).
CONCUR=50

SHELL=/bin/bash -o pipefail -c
#.SHELLFLAGS=-o pipefail -c
TMPDIR=/tmp

PSH=bash -o pipefail -c
## We prefer GNU split. We require a split that takes the -d argument.
SPLIT=$(shell which gsplit 2>/dev/null || which split || echo split)
## Galago requires this.
JAVA_HOME?=$(dir $(patsubst %/,%,$(dir $(shell which javac || which java))))
export JAVA_HOME

passim=passim
passimjar=$(shell which $(passim))
passimopt=-server -Dfile.encoding=UTF-8 -jar $(passimjar)

## Pairwise detection parameters
UPPER=100
NGRAM=5
MAXDF=$(UPPER)
MODP=1
MODREC=1
GAP=1
WIDTH=1
MINREP=5
STRIDE=1000000

BINS=128
bins=$(shell seq 0 $$(( $(BINS) - 1 )))

## Alignment parameters
MINMINALG=10

## Clustering parameters
MINALG=20
RELOVER=0.5
MAXREP=4

ifeq ($(MODP),1)
pairs=pairs-n$(NGRAM)u$(UPPER)
else
MINREP=1
pairs=pairs-n$(NGRAM)u$(UPPER)p$(MODP)
endif

ifneq ($(MODREC),1)
pairs:=$(pairs)r$(MODREC)
MINREP=1
endif

ifneq ($(MAXDF),$(UPPER))
pairs:=$(pairs)d$(MAXDF)
endif

ifeq ($(NGRAM),1)
idx=$(COLL)/idx/postings
else
idx=$(COLL)/idx/od.n$(NGRAM).w1.h2.df.pos
endif

stops=

ifneq ($(GAP),1)
pairs:=$(pairs)w$(WIDTH)g$(GAP)
idx=$(COLL)/idx/od.n$(NGRAM).w$(WIDTH).h2.g$(GAP).s5.df.pos
stops=--stop $(COLL)/df05.stop
endif

ifneq ($(MINREP),5)
pairs:=$(pairs)m$(MINREP)
endif

ifneq ($(wildcard $(COLL)/idx/names.smap*),)
pairopts+= --series-map $(COLL)/idx/names.smap
endif

dir=$(COLL)/$(pairs)

cluster=$(dir)/cl$(MINALG)-flover$(subst .,,$(RELOVER))-rep$(MAXREP).cluster.gz
clinfo=$(patsubst %.cluster.gz,%.clinfo.gz,$(cluster))
idtab=$(patsubst %.cluster.gz,%.idtab.gz,$(cluster))
gexf=$(patsubst %.cluster.gz,%.gexf.gz,$(cluster))

.PHONY:	clinfo cluster gexf idtab swalign align pairs index clean distclean

.SECONDARY:

clinfo:	$(clinfo)

idtab:	$(idtab)

gexf:	$(gexf)

cluster:	$(cluster)

align:	$(dir)/pall.proc.srt.gz

swalign:	$(dir)/pall.sw.srt.gz

pairs:	$(dir)/bins/job

index:	$(idx)

clean:
	$(RM) -r $(COLL)/postings.tmp $(COLL)/$(notdir $(idx)).tmp $(dir)/pall.srt.tmp $(dir)/pall.proc.tmp $(dir)/pall.sw.tmp $(dir)/cl*.clinfo.tmp

distclean:
	$(RM) -r $(COLL)/postings.tmp $(COLL)/$(notdir $(idx)).tmp $(COLL)/idx $(COLL)/pairs-*

# .DELETE_ON_ERROR:

GDISTRIB=$(strip $(shell tops=`ls -1 $(COLL)/input | wc -l`; if [ $$tops -gt 100 ]; then echo 100; else echo $$tops; fi))

%/idx/postings:
	cd $*; ${galagoIndex} build ${CURDIR}/build.json ${galagoMode} --distrib=$(GDISTRIB) --galagoJobDir=$(notdir $@).tmp

$(idx):	$(dir $(idx))postings
	cd $(COLL); ${galagoIndex} build-window ${CURDIR}/window.json --n=$(NGRAM) --width=$(WIDTH) --gap=$(GAP) ${galagoMode} --distrib=$(GDISTRIB) --galagoJobDir=$(notdir $@).tmp --outputIndexName=$(notdir $@)

## Separate target to avoid race condition in parallel make.
$(dir)/bins/0:
	mkdir -p $(foreach b,$(bins),$(dir)/bins/$(b))

$(dir)/bins/step%.out:
	$(QRUN) '${passim} pairs $(stops) --max-series $(UPPER) --max-df $(MAXDF) --modp $(MODP) --modrec $(MODREC) --step $* --stride ${STRIDE} --hash-bins ${BINS} $(pairopts) $(idx) | ./fsplit.pl $(dir)/bins $*.gz'; touch $@

$(dir)/bins/job:	$(dir)/od.siz $(dir)/bins/0
	$(MAKE) $(foreach s,$(shell seq 0 `cat $(dir)/od.siz`),$(dir)/bins/step$(s).out)

algram=$(NGRAM)
ifneq ($(GAP),1)
algram=1
endif

$(dir)/bins/%/pall.proc.tmp/in000.algin.gz:
	mkdir -p $(dir)/bins/$*/pall.proc.tmp
	$(QRUN) 'gzip -fdc $(dir)/bins/$*/[0-9]*.gz | sort | ${passim} merge -m ${MINREP} | ${SPLIT} -d -a 3 -l 10000 - $(dir)/bins/$*/pall.proc.tmp/in; touch $(dir)/bins/$*/pall.proc.tmp/in000; for i in $(dir)/bins/$*/pall.proc.tmp/in*[0-9]; do gzip $$i; mv $$i.gz $$i.algin.gz; done'

%.algout.gz:	%.algin.gz
	$(QRUN) -R 'mem>16000' 'gzip -dc $< | java -Xmx15900m ${passimopt} scores -n $(algram) $(dir)/../idx | awk "\$$1 >= $(MINMINALG)" | sort -rn | gzip -c > $@'

%.tmp/job:	%.tmp/in000.algin.gz
	$(MAKE) $(subst .algin.gz,.algout.gz,$(shell if [ -f $< ]; then echo `ls -1 $*.tmp/in*.algin.gz`; else echo $<; fi))

$(dir)/bins/%/pall.proc.srt.gz:	$(dir)/bins/%/pall.proc.tmp/in000.algin.gz $(dir)/bins/%/pall.proc.tmp/job
	$(QRUN) 'ls -1 $(dir)/bins/$*/pall.proc.tmp/in*.algout.gz | xargs gzip -dc | sort -rn | gzip -c > $@'

%.tmp/job:	%.tmp/in000
	$(MAKE) $(subst /in,/out,$(shell if [ -f $< ]; then echo `ls -1 $*.tmp/in*`; else echo $<; fi))

$(dir)/pall.proc.srt.gz:	$(patsubst %,$(dir)/bins/%/pall.proc.srt.gz,$(bins))
	$(QRUN) 'ls -1 $(dir)/bins/*/pall.proc.srt.gz | xargs gzip -dc | sort -rn | gzip -c > $@'

$(dir)/pall.sw.tmp/in000:	$(dir)/pall.srt.gz
	mkdir -p $(dir)/pall.sw.tmp
	gzip -fdc $< | ${SPLIT} -d -a 3 -l 20000 - $(dir)/pall.sw.tmp/in

$(dir)/pall.sw.tmp/out%:	$(dir)/pall.sw.tmp/in%
	java -Xmx7900m ${passimopt} scores -n 0 $(dir)/../idx < $< | awk "\$$1 >= $(MINMINALG)" | sort -rn > $@

$(dir)/pall.sw.srt.gz:	$(dir)/pall.srt.gz $(dir)/pall.sw.tmp/in000 $(if ${q},,$(dir)/pall.sw.tmp/job)
ifeq (${q},lsf)
	${sub} -R 'mem>8000' -J "$(dir)/pall.sw[1-`ls -1 $(dir)/pall.sw.tmp/in* | wc -l`]%${CONCUR}" -eo "$(dir)/pall.sw.tmp/err%I" \
	    'ofile=$(dir)/pall.sw.tmp/out`printf "%03d" $$(( \$$LSB_JOBINDEX - 1 ))`; [ -f $$ofile ] || java -Xmx7900m ${passimopt} scores -n 0 $(dir)/../idx < $(dir)/pall.sw.tmp/in`printf "%03d" $$(( \$$LSB_JOBINDEX - 1 ))` | awk "\$$1 >= $(MINMINALG)" | sort -rn > $$ofile'
	${sub} -K -w "$(dir)/pall.sw" 'ls -1 $(dir)/pall.sw.tmp/out* | xargs sort -rnm | gzip -c > $@'
else ifeq (${q},sge)
	${sub} -N $(subst /,_,$(dir)/pall.sw) -t "1-`ls -1 $(dir)/pall.sw.tmp/in* | wc -l`:1" -tc ${CONCUR} -o $(dir)/pall.sw.tmp \
	    'ofile=$(dir)/pall.sw.tmp/out`printf "%03d" $$(( \$$SGE_TASK_ID - 1 ))`; [ -f $$ofile ] || java -Xmx7900m ${passimopt} scores -n 0 $(dir)/../idx < $(dir)/pall.sw.tmp/in`printf "%03d" $$(( \$$SGE_TASK_ID - 1 ))` | awk "\$$1 >= $(MINMINALG)" | sort -rn > $$ofile'
	${sub} -sync y -hold_jid $(subst /,_,$(dir)/pall.sw) -o $(dir)/pall.sw.tmp 'ls -1 $(dir)/pall.sw.tmp/out* | xargs sort -rnm | gzip -c > $@'
else
	ls -1 $(dir)/pall.sw.tmp/out* | xargs sort -rnm | gzip -c > $@
endif

%.clinfo.tmp/in000:	%.cluster.gz
	mkdir -p $*.clinfo.tmp; \
	gzip -fdc $< | ${SPLIT} -d -a 3 -l 2000 - $*.clinfo.tmp/in

$(subst .gz,.tmp,$(clinfo))/out%:	$(subst .gz,.tmp,$(clinfo))/in%
	${passim} format $(dir $@)/../../idx < $< > $@

%.clinfo.gz:	%.clinfo.tmp/in000 $(if ${q},,%.clinfo.tmp/job)
ifeq (${q},lsf)
	${sub} -J "$*.clinfo[1-`ls -1 $*.clinfo.tmp/in* | wc -l`]%${CONCUR}" -eo "$*.clinfo.tmp/err%I" \
	    'ofile=$*.clinfo.tmp/out`printf "%03d" $$(( \$$LSB_JOBINDEX - 1 ))`; [ -f $$ofile ] || ${passim} format $(dir $@)/../idx < $*.clinfo.tmp/in`printf "%03d" $$(( \$$LSB_JOBINDEX - 1 ))` > $$ofile'
	${sub} -K -w "$*.clinfo" 'ls -1 $*.clinfo.tmp/out* | xargs cat | gzip -c > $@'
else ifeq (${q},sge)
	${sub} -N $(subst /,_,$*.clinfo) -t `ls -1 $*.clinfo.tmp/in* | wc -l` -tc ${CONCUR} -o $*.clinfo.tmp \
	    'ofile=$*.clinfo.tmp/out`printf "%03d" $$(( \$$SGE_TASK_ID - 1 ))`; [ -f $$ofile ] || ${passim} format $(dir $@)/../idx < $*.clinfo.tmp/in`printf "%03d" $$(( \$$SGE_TASK_ID - 1 ))` > $$ofile'
	${sub} -sync y -hold_jid $(subst /,_,$*.clinfo) -o $*.clinfo.tmp 'ls -1 $*.clinfo.tmp/out* | xargs cat | gzip -c > $@'
else
	ls -1 $*.clinfo.tmp/out* | xargs cat | gzip -c > $@
endif

$(cluster):	$(dir)/pall.proc.srt.gz
	$(QRUN) 'gzip -dc $< | awk "\$$1 >= $(MINALG)" | java -d64 -Xmx32g ${passimopt} cluster --relative-overlap $(RELOVER) --max-repeats $(MAXREP) | gzip -c > $@'

%.idtab.gz:	%.clinfo.gz
	$(QRUN) 'gzip -dc $< | ${passim} idtab | sort -k1,1n -k3,4 | gzip -c > $@'

%.gexf.gz:	%.clinfo.gz publications.json
	$(QRUN) 'gzip -dc $< | ${passim} gexf publications.json | gzip -c > $@'

$(dir)/od.siz:	$(idx)
	mkdir -p $(dir)
	galago dump-index-manifest $< | grep "keyCount" | perl -lne 'print int($$1 / ${STRIDE}) if /(\d+)/' > $@

%/df05.stop:	%/idx/postings
	galago dump-term-stats $< | awk "\$$3 >= $$(( `galago dump-index-manifest $*/idx/names | grep "keyCount" | perl -lane 'print $$1 if / (\d+),/'` / 2 ))" | sort -k3,3rn -k2,2rn | cut -f 1 > $@

%/df01.stop:	%/idx/postings
	galago dump-term-stats $< | awk "\$$3 >= $$(( `galago dump-index-manifest $*/idx/names | grep "keyCount" | perl -lane 'print $$1 if / (\d+),/'` / 10 ))" | sort -k3,3rn -k2,2rn | cut -f 1 > $@
