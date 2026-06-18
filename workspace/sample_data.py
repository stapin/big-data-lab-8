import pandas as pd
import csv

# Список колонок, которые мы ищем
target_columns = ["product_name", "energy-kcal_100g", "proteins_100g", "fat_100g", "carbohydrates_100g"]

print("Читаем датасет чанками без обработки кавычек...")

chunks = []

# quoting=csv.QUOTE_NONE (или просто 3) заставляет pandas игнорировать кавычки 
# и воспринимать их как обычный текст внутри строки
with pd.read_csv(
    "workspace/openfoodfacts.csv.gz", 
    sep='\t', 
    usecols=target_columns, 
    compression='gzip',
    on_bad_lines='skip', 
    chunksize=50000,
    quoting=csv.QUOTE_NONE,
    low_memory=False
) as reader:
    
    for chunk in reader:
        # Берем 1% от текущего чанка и удаляем пустые значения
        chunk_sample = chunk.sample(frac=0.01, random_state=42).dropna()
        chunks.append(chunk_sample)

print("Объединяем сэмплы...")
df_sample = pd.concat(chunks, ignore_index=True)

# Сохраняем результат
df_sample.to_csv("workspace/sample_data.csv", index=False)

print(f"Готово! Размер файла: {df_sample.shape}. Колонки: {list(df_sample.columns)}")