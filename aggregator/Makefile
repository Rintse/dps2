# Java stuff
COMPILE_LIBS	= $(shell find compile_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/:/g')
RUN_LIBS        = $(shell find run_libs/ -name "*.jar" | sed ':a;N;$$!ba;s/\n/,/g')
SRC_DIR         = ./aggregation
SRC_FILES       = $(shell find $(SRC_DIR) -name "*.java")
JAR_NAME        = ./aggregate.jar

# Command line arguments
INPUT_ADRESS    = localhost
INPUT_PORT      = 5555
MONGO_ADRESS    = localhost
MONGO_LAT_ADRESS= localhost
NUM_WORKERS     = 1
NUM_STREAMS     = 1
GEN_RATE        = 1
STORM_CONF      = ~/storm/conf/storm.yaml

all: build

build: $(SRC_FILES)
	make clean
	javac -Xlint:unchecked -cp $(COMPILE_LIBS) $(SRC_FILES)
	@jar cvf $(JAR_NAME) $(SRC_DIR)

submit: build
	storm jar --config $(STORM_CONF) --jars $(RUN_LIBS) $(JAR_NAME) aggregation.AggregateVotes \
	$(INPUT_ADRESS) \
	$(INPUT_PORT) \
	$(MONGO_ADRESS) \
	$(MONGO_LAT_ADRESS) \
	$(NUM_WORKERS) \
	$(NUM_STREAMS) \
	$(GEN_RATE)

clean:
	rm -f $(JAR_NAME)
	rm -f $(SRC_DIR)/*.class
