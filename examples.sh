#!/bin/bash
# Scripts de ejemplo para automatización del sistema de expenses

# ===============================================================
# SCRIPT 1: Procesamiento automático de próximo lote
# ===============================================================
echo "🚀 Ejemplo 1: Procesamiento automático"
echo "Procesa automáticamente los próximos 10 expenses"
python main.py auto

# ===============================================================
# SCRIPT 2: Procesamiento de lote personalizado  
# ===============================================================
echo "🚀 Ejemplo 2: Lote personalizado de 20 expenses"
python main.py auto --batch-size 20

# ===============================================================
# SCRIPT 3: Procesamiento de rango específico
# ===============================================================
echo "🚀 Ejemplo 3: Rango específico 100-150"
python main.py range 100 150

# ===============================================================
# SCRIPT 4: Procesamiento continuo (para producción)
# ===============================================================
echo "🚀 Ejemplo 4: Procesamiento continuo"
echo "Procesa lotes de 15 expenses cada 45 segundos, máximo 10 lotes"
python main.py continuous --batch-size 15 --delay 45 --max-batches 10

# ===============================================================
# SCRIPT 5: Solo extracción específica
# ===============================================================
echo "🚀 Ejemplo 5: Solo extracción 1-100"
python main.py extract 1 100

# ===============================================================
# SCRIPT 6: Solo procesamiento de datos ya extraídos
# ===============================================================
echo "🚀 Ejemplo 6: Procesar datos ya extraídos 1-100"
python main.py process 1 100

# ===============================================================
# SCRIPT 7: Procesamiento continuo para testing
# ===============================================================
echo "🚀 Ejemplo 7: Procesamiento rápido para testing"
python main.py continuous --batch-size 5 --delay 5 --max-batches 3
