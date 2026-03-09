# PSet 2 — Data Warehouse de Viajes en Taxi

## Autor
Emilio Soria - 00326990

---

# 1. Descripción General

Este proyecto implementa un **Data Warehouse para viajes en taxi de Nueva York**.

Etapas del pipeline:

1. Ingesta de datos en la **capa Bronze**
2. Limpieza y normalización de datos en la **capa Silver**
3. Esquema estrella analítico en la **capa Gold**
4. Consultas analíticas para obtener insights de negocio

Tecnologías utilizadas:

- PostgreSQL
- Mage AI
- dbt
- Docker
- Python (psycopg2)
- Jupyter Notebook

---

# 2. Arquitectura

El sistema sigue una **Arquitectura Medallion**:

Datos Crudos → Bronze → Silver → Gold → Analytics

### Capas

| Layer | Description |
|-----|-----|
| Bronze | Datos crudos de taxis cargados en PostgreSQL |
| Silver | Registros de viajes limpiados y estandarizados |
| Gold | Esquema estrella optimizado para análisis |

---

# 3. Arquitectura del Pipeline

<img width="493" height="613" alt="Captura de pantalla 2026-03-09 000015" src="https://github.com/user-attachments/assets/c5bf17a2-8348-4f87-b4af-cf62978d610c" />
<img width="301" height="430" alt="Captura de pantalla 2026-03-09 000023" src="https://github.com/user-attachments/assets/99404d23-cce2-4f82-b964-853bdb9022a9" />
<img width="490" height="452" alt="Captura de pantalla 2026-03-09 000103" src="https://github.com/user-attachments/assets/087c7f29-7674-4c2b-a872-c09e818a29de" />



Orden del pipeline:

ingest_bronze  
↓  
dbt_build_silver  
↓  
dbt_build_gold  

---

# 4. Modelo de Datos

La **capa Gold implementa un Esquema Estrella (Star Schema)**.

## Tabla de Hechos

### gold.fct_trips

Almacena todos los eventos a nivel de viaje.

| Column | Description |
|------|------|
| trip_key | Identificador único del viaje |
| pickup_date_key | Clave foránea hacia dim_date |
| pickup_date | Fecha del viaje |
| pu_zone_key | Zona de recogida |
| do_zone_key | Zona de destino |
| service_type | Yellow o Green |
| payment_type | Método de pago |
| vendor_id | Proveedor del taxi |
| passenger_count | Número de pasajeros |
| trip_distance | Distancia recorrida |
| total_amount | Costo total del viaje |

---

## Tablas de Dimensión

### gold.dim_zone

| Column | Description |
|------|------|
| zone_key | ID de ubicación |
| borough | Distrito de NYC |
| zone | Nombre de la zona |
| service_zone | Región de servicio |

---

### gold.dim_date

| Column | Description |
|------|------|
| date_key | YYYYMMDD |
| date | Fecha del calendario |
| year | Año |
| month | Mes |
| day | Día |
| day_of_week | Día de la semana |
| quarter | Trimestre |

---

### gold.dim_service_type

| Column | Description |
|------|------|
| service_type_key | Identificador |
| service_type | yellow / green |

---

### gold.dim_payment_type

| Column | Description |
|------|------|
| payment_type_key | Identificador |
| payment_type | card / cash / other |

---

# 5. Particionamiento de Tablas

La tabla de hechos está **particionada por fecha de recogida (pickup date)**.

Ejemplo de particiones:

<img width="1339" height="786" alt="image" src="https://github.com/user-attachments/assets/89b5aced-7e0d-4f7e-8f9a-6cceefff6a13" />
<img width="1147" height="340" alt="image" src="https://github.com/user-attachments/assets/66e48379-2e91-4246-a269-f904c27c68a4" />
<img width="1104" height="417" alt="image" src="https://github.com/user-attachments/assets/15f9a0ec-ebbc-44ee-9157-1842c81e7751" />



---

# 6. Reglas de Limpieza de Datos (Capa Silver)

Reglas de limpieza aplicadas:

- Eliminar viajes con timestamps NULL
- Asegurar que la recogida ocurra antes que el destino
- Eliminar distancias negativas
- Eliminar montos totales negativos
- Restringir viajes a rangos de fechas válidos

---

# 7. Transformaciones dbt

### Modelos Silver

| Model | Description |
|------|------|
| silver_trips | Datos de viajes limpiados |
| silver_taxi_zones | Tabla de zonas de taxi limpiada |

### Modelos Gold

| Model | Description |
|------|------|
| dim_date | Dimensión de fechas |
| dim_zone | Dimensión de zonas |
| dim_service_type | Tipo de taxi |
| dim_payment_type | Métodos de pago |
| fct_trips | Tabla de hechos |

---

# 8. Pruebas de Calidad de Datos

Las pruebas de dbt aseguran la integridad de los datos.

Ejemplos:

- not_null
- accepted_values
- integridad referencial

Ejemplo:

```yaml
tests:
  - not_null
  - accepted_values
```
<img width="843" height="722" alt="image" src="https://github.com/user-attachments/assets/cc2c5a4b-bc29-459d-ae05-bb5f55d12e33" />

---

# 9. Consultas Analíticas

El data warehouse soporta preguntas analíticas como:

- Viajes por mes
- Ingresos por tipo de servicio
- Zonas con mayor número de recogidas
- Análisis del porcentaje de propina
- Estadísticas de duración de viajes

Las consultas fueron implementadas usando **psycopg2 en un Jupyter Notebook**.

---

# 10. Resultados


---


# 11. Reproducibilidad

Todo el sistema está containerizado usando Docker.

Para ejecutar el proyecto:

```bash
docker compose up --build
```

Luego acceder a los pipelines de Mage y ejecutarlos en orden.

---

# 13. Estructura del Repositorio

```
project/
│
├── docker-compose.yml
├── Dockerfile
├── scheduler_data/
├── notebooks/
├── README.md
├── create_partitions.sql
├── requirements.txt
```

---
