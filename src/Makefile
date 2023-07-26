clean:
	rm mr-*-*.txt
clean_all:
	rm mr-*.txt
	rm final.txt
generate:
	rm mr-*-*.txt
	cat mr-* | sort > final.txt
count:
	@find . -name "*.rs" | xargs wc -l | tail -n 1
