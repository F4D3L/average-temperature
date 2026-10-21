import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Fase de Map: lê uma linha do CSV e emite {@code ("temperatura", (temp, 1))}.
 *
 * <p>Chave/valor de entrada: posição (offset em bytes) da linha no arquivo / a própria linha (vindos
 * do {@code TextInputFormat}). Chave/valor de saída: {@link Text} / {@link SumCountWritable}.
 *
 * <p>Todas as leituras são emitidas com a mesma <b>chave global</b>. O sort/shuffle agrupa os
 * valores por chave, então uma chave única leva todas as leituras ao mesmo reducer, que é o que uma
 * média global precisa.
 *
 * <p>O arquivo não tem linha de cabeçalho. Colunas: X, Y, month, day, FFMC, DMC, DC, ISI, temp, RH,
 * wind, rain, area; portanto a temperatura está no índice 8 (começando em 0).
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {

    static final int TEMPERATURE_INDEX = 8;

    /** Contadores do Hadoop, exibidos no resumo do job, para linhas que não puderam ser usadas. */
    enum Readings {
        MALFORMED
    }

    private static final Text GLOBAL_KEY = new Text("temperatura");

    // Reutilizado entre as chamadas: o context.write serializa o valor na hora, então é seguro.
    private final SumCountWritable reading = new SumCountWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] fields = line.toString().split(",");
        if (fields.length <= TEMPERATURE_INDEX) {
            context.getCounter(Readings.MALFORMED).increment(1);
            return;
        }

        double temperature;
        try {
            temperature = Double.parseDouble(fields[TEMPERATURE_INDEX].trim());
        } catch (NumberFormatException e) {
            // Um cabeçalho ou uma linha corrompida: ignora a linha em vez de derrubar o job inteiro.
            context.getCounter(Readings.MALFORMED).increment(1);
            return;
        }

        // Valor composto: esta leitura é uma soma de `temperature` sobre 1 leitura.
        reading.set(temperature, 1);
        context.write(GLOBAL_KEY, reading);
    }
}
