-- Tabla para datos processados con Spark
CREATE TABLE IF NOT EXISTS public.embalses_high_volume (
    embalse VARCHAR(255) NOT NULL,
    dias_alto_volumen INT NOT NULL
);

-- Tabla para datos processados con Trino
CREATE TABLE IF NOT EXISTS public.embalses_aggregated (
    embalse VARCHAR(255),
    fecha DATE,
    max_nivel_absoluto FLOAT,
    max_porcentaje FLOAT,
    max_volumen FLOAT
);