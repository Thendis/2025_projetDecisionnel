
setup_env:
	export PATH=/usr/gide/sbt-1.3.13/bin:/usr/gide/jdk-1.8/bin:$PATH

build :
	sbt "runMain Business_cea"
	sbt "runMain Business_dwh"
	sbt -J-Xmx16g "runMain Review_dwh"
	echo "===> RUNNING OK"

connect_source:
	psql -h stendhal.iem -U tpid tpid2020

connect_cible:
	psql -h kafka.iem -U an450821 an450821

git_add:
	git add build.sbt
	git add .gitignore
	git add README.md
	git add Makefile
	git add src/main/scala/*
	git add *.sql
	git add SQL/*
	git add docs/*
