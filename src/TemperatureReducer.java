import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Fase de Reduce: recebe {@code ("temperatura", [(t1, 1), (t2, 1), ...])} depois do sort/shuffle e
 * emite {@code ("temperatura", média)}.
 *
 * <p>Chave/valor de entrada: {@link Text} / {@link SumCountWritable}. Chave/valor de saída:
 * {@link Text} / {@link DoubleWritable}.
 *
 * <p>A fórmula é {@code (soma de todas as temperaturas) / (quantidade de leituras)}: os dois totais
 * são acumulados primeiro e divididos uma única vez, no final. Tirar a média das médias parciais
 * daria um resultado errado sempre que as partes tivessem quantidades diferentes de leituras
 * (Aula 05).
 */
public class TemperatureReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        long count = 0;
        // Soma a quantidade de cada valor em vez de 1, então somas parciais também funcionam (por exemplo, vindas de um Combiner).
        for (SumCountWritable value : values) {
            sum += value.getSum();
            count += value.getCount();
        }

        if (count > 0) {
            context.write(key, new DoubleWritable(sum / count));
        }
    }
}
