# Temperatura Média — MapReduce

Atividade de Big Data (PUCPR): cálculo da temperatura média do arquivo `forestfireinput.csv`
com Hadoop MapReduce.

## Como funciona

O arquivo não tem cabeçalho, e a temperatura está na coluna 8 (contando a partir de 0). Todas as
leituras vão para a mesma chave global com um valor composto `(soma, quantidade)`, e a divisão
acontece uma única vez, no reducer. Tirar a média das médias parciais daria um resultado errado
sempre que as partes tivessem quantidades diferentes de leituras (Aula 05).

```
Map:     "7,5,mar,fri,...,8.2,..."  ->  ("temperatura", (8.2, 1))
Shuffle: agrupa pela chave          ->  ("temperatura", [(8.2, 1), (18.0, 1), ...])
Reduce:  soma(temp) / soma(quant.)  ->  ("temperatura", 18.889168278529986)
```

| Classe | Papel |
| --- | --- |
| `AverageTemperature` | Driver (`main`): configura e submete o job |
| `TemperatureMapper` | Map: lê uma linha e emite `("temperatura", (temp, 1))` |
| `TemperatureReducer` | Reduce: soma as temperaturas e as quantidades e emite a média |
| `SumCountWritable` | `Writable` customizado para o valor composto `(soma, quantidade)` |

Resultado: **18,889168278529986 °C** sobre 517 leituras (soma 9765,7).

## Como executar

Requisitos: Maven e **Java 8, 11 ou 17**. O Hadoop 3.3.6 não roda no Java 23 ou superior.

```bash
mvn package                                   # gera target/average-temperature-1.0.0.jar

# Em uma máquina/VM com Hadoop instalado (o diretório de saída não pode existir):
hdfs dfs -put input/forestfireinput.csv /forestfire/input.csv
hadoop jar target/average-temperature-1.0.0.jar /forestfire/input.csv /forestfire/output
hdfs dfs -cat /forestfire/output/part-r-00000

# Modo local do Hadoop, sem cluster (Linux/macOS; no Windows o Hadoop também exige o winutils.exe):
mvn compile exec:java                         # lê input/ e grava em target/output/
cat target/output/part-r-00000
```
