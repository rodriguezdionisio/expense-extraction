#!/usr/bin/env python3
"""
Script de prueba para verificar que los names se incluyen en el procesamiento.
"""

import json
import pandas as pd
from expense_processor import ExpenseProcessor

def test_names_extraction():
    """Prueba que los names se extraigan correctamente."""
    
    # Crear instancia del procesador
    processor = ExpenseProcessor()
    
    # Leer un archivo JSON de prueba
    with open('raw/expense_7.json', 'r', encoding='utf-8') as f:
        expense_data = json.load(f)
    
    print("📄 Datos originales del JSON:")
    print("="*50)
    
    # Mostrar las relaciones originales
    relationships = expense_data['data']['relationships']
    included = expense_data['included']
    
    print("🔗 Relaciones (solo IDs):")
    for rel_name, rel_data in relationships.items():
        if rel_data.get('data'):
            data = rel_data['data']
            if isinstance(data, list):
                for item in data:
                    rel_id = item.get('id')
                    rel_type = item.get('type')
                    print(f"  {rel_name}: ID={rel_id}, Type={rel_type}")
            else:
                rel_id = data.get('id')
                rel_type = data.get('type')
                print(f"  {rel_name}: ID={rel_id}, Type={rel_type}")
    
    print("\n📋 Objetos incluidos (con names):")
    for item in included:
        if item['type'] in ['CashRegister', 'PaymentMethod', 'Provider', 'User']:
            item_id = item['id']
            item_name = item['attributes'].get('name', 'N/A')
            print(f"  {item['type']} {item_id}: {item_name}")
    
    print("\n" + "="*50)
    print("🔄 Procesando con el código modificado...")
    
    # Procesar con el código modificado
    expense, items = processor._process_expense(expense_data)
    
    if expense:
        print("\n✅ Resultado del procesamiento:")
        print("="*50)
        
        # Mostrar campos relacionados con keys y names
        for field_name, field_value in expense.items():
            if '_key' in field_name or '_name' in field_name:
                print(f"  {field_name}: {field_value}")
        
        print(f"\n📊 Creando DataFrame para verificar estructura...")
        df = pd.DataFrame([expense])
        print(f"Columnas del DataFrame: {len(df.columns)}")
        
        # Mostrar específicamente las columnas de names
        name_columns = [col for col in df.columns if '_name' in col]
        if name_columns:
            print(f"\n✅ Columnas de names encontradas: {name_columns}")
            for col in name_columns:
                print(f"  {col}: {df[col].iloc[0]}")
        else:
            print("\n❌ No se encontraron columnas de names")
            
        # Mostrar todas las columnas para debug
        print(f"\n📋 Todas las columnas del DataFrame:")
        for col in sorted(df.columns):
            print(f"  {col}: {df[col].iloc[0]}")
            
    else:
        print("❌ Error procesando el expense")

if __name__ == "__main__":
    test_names_extraction()
