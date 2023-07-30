clean:
	rm mr-*-*.txt
	rm *.nohup
clean_all:
	rm mr-*.txt
	rm *.nohup
	rm final.txt
generate:
	rm mr-*-*.txt
	cat mr-* | sort > final.txt
count:
	@find . -name "*.rs" | xargs wc -l | tail -n 1
