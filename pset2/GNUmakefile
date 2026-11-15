# Provided for convenience; delegates immediately to cmake

cmake_bool = $(if $(filter 1 on,$(1)),ON,$(if $(filter 0 off,$(1)),OFF,$(1)))

cmake_build := -DSAN=$(call cmake_bool,$(SAN)) \
	-DASAN=$(call cmake_bool,$(ASAN)) \
	-DUBSAN=$(call cmake_bool,$(UBSAN)) \
	-DTSAN=$(call cmake_bool,$(TSAN))

ifeq ($(V),1)
cmake_verbose := --verbose
endif

targets = ping ctconsensus ctstubborn ctfail

all:
	cmake -B build $(cmake_build)
	cmake --build build $(cmake_verbose)

clean:
	rm -rf build .cache

$(targets):
	cmake -B build $(cmake_build)
	cmake --build build --target $@ $(cmake_verbose)

$(targets:%=build/%): build/%:
	cmake -B build $(cmake_build)
	cmake --build build --target $* $(cmake_verbose)

test: ctconsensus
	build/ctconsensus -q -R 10000

.PHONY: all clean test $(targets) $(targets:%=build/%)
