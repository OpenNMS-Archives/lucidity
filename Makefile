
all: index.html tutorial.html

index.html:
	cat head.tmpl > index.html
	cat index-content.html >> index.html
	cat tail.tmpl >> index.html

tutorial.html:
	cat head.tmpl > tutorial.html
	cat tutorial-content.html >> tutorial.html
	cat tail.tmpl >> tutorial.html

clean:
	-rm index.html tutorial.html