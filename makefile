PATH := ./node_modules/.bin:${PATH}

.PHONY : init clean-docs clean build test dist publish

init:
	npm install

build:
	coffee -o lib/ -c src/

clean:
	rm -rf lib/

dist: clean init build

publish: dist
	npm publish