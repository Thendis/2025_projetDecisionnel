
setup:
	export PATH=/usr/gide/sbt-1.3.13/bin:/usr/gide/jdk-1.8/bin:$PATH

build :
	sbt "runMain Business_cea"
	sbt "runMain Business_dwh"
	sbt "runMain Business_planning_axe"
	echo "===> RUNNING OK"

git_add:
	git add build.sbt
	git add .gitignore
	git add README.md
	git add Makefile
	git add src/main/scala/*
	git add *.sql
	git add SQL/*
	git add docs/*