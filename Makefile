
all: index.html

index.html:
	cat head.tmpl > index.html
	cat index-content.html >> index.html
	cat tail.tmpl >> index.html

clean:
	-rm index.html