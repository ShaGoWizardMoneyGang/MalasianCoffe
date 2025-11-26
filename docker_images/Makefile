# Si queres anadir una imagen de docker, solo necesitas anadir un directorio con
# un Dockerfile. El nombre de la imagen va a ser igual al nombre del directorio.
IMAGES := $(addprefix docker-build-, $(patsubst %/,%,$(wildcard */)))

all: $(IMAGES)

docker-build-%:
	docker build -t $* -f $*/Dockerfile ../
