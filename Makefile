

build :
	sbt "runMain Business_cea"
	sbt "runMain Business_dwh"
	sbt -J-Xmx16g "runMain Review_dwh"
	sbt -J-Xmx16g "runMain Exterior_axe"
	echo "===> RUNNING OK"

connect_source:
	psql -h stendhal.iem -U tpid tpid2020

connect_cible:
	psql -h kafka.iem -U em963948 em963948

git_add:
	git add build.sbt
	git add .gitignore
	git add README.md
	git add Makefile
	git add src/main/scala/*
	git add *.sql
	git add SQL/*
	git add docs/*
