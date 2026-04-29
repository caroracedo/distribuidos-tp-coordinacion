## Fix - Unificar lógica de orden utilizando FruitItem en Joiner

Al cambiar la implementación de `FruitItem` detecté que el `Joiner` mantenía una lógica hardcodeada que priorizaba los valores máximos, lo que generaba un comportamiento inconsistente con el criterio de orden definido.

La corrección consistió en unificar la lógica utilizando `FruitItem` también dentro del `Joiner`, evitando depender de comparaciones manuales sobre valores primitivos. De esta manera, el criterio de orden queda centralizado y es fácilmente modificable, lo cual mejora la consistencia y mantenibilidad del sistema. Este ajuste queda incorporado y será tenido en cuenta para futuros desarrollos.
