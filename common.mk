# Dependencies: git pandoc moreutils httpie twine

SHELL=/bin/bash -eo pipefail

release_major:
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*' | perl -ne '/^v(\d)+\.(\d)+\.(\d+)+/; print "v@{[$$1+1]}.0.0"'))
	$(MAKE) release

release_minor:
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*' | perl -ne '/^v(\d+)\.(\d+)\.(\d+)+/; print "v$$1.@{[$$2+1]}.0"'))
	$(MAKE) release

release_patch:
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*' | perl -ne '/^v(\d)+\.(\d)+\.(\d+)+/; print "v$$1.$$2.@{[$$3+1]}"'))
	$(MAKE) release

release:
	@if [[ $$(which twine) ]]; then :; else echo "*** Please install dependencies with 'pip install -r requirements-dev.txt' ***"; exit 1; fi
	@if [[ -z $$TAG ]]; then echo "Use release_{major,minor,patch}"; exit 1; fi
	git pull
	$(MAKE) clean
	TAG_MSG=$$(mktemp); \
	    echo "# Changes for ${TAG} ($$(date +%Y-%m-%d))" > $$TAG_MSG; \
	    git log --pretty=format:%s $$(git describe --abbrev=0)..HEAD >> $$TAG_MSG; \
	    $${EDITOR:-vi} $$TAG_MSG; \
	    if [[ -f Changes.md ]]; then cat $$TAG_MSG <(echo) Changes.md | sponge Changes.md; git add Changes.md; fi; \
	    if [[ -f Changes.rst ]]; then cat <(pandoc --from markdown --to rst $$TAG_MSG) <(echo) Changes.rst | sponge Changes.rst; git add Changes.rst; fi; \
	    git commit -m ${TAG}; \
	    git tag --annotate --file $$TAG_MSG ${TAG}
	git push --follow-tags
	$(MAKE) version
	$(MAKE) pypi_release

pypi_release:
	$(MAKE) sdist
	twine upload dist/*

undo:
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*'))
	@echo -e "DANGEROUS - DO NOT USE!\nAbout to delete tag/commit for $(TAG)\nPress enter to continue or Ctrl-C to exit."
	@read x
	$(eval export TAG=$(shell git describe --tags --match 'v*.*.*'))
	@if [[ -z $$TAG ]]; then echo "Can't find last release"; exit 1; fi
	git push origin :refs/tags/$(TAG)
	git tag -d $(TAG)
	git reset --hard HEAD^
	git push -f

.PHONY: release
