import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Valor composto enviado do Map para o Reduce: uma soma parcial de temperaturas e a quantidade de
 * leituras que essa soma representa.
 *
 * <p>Não dá para obter a média tirando a média das médias parciais, porque cada parte pode ter uma
 * quantidade diferente de leituras (Aula 05: 9,42 em vez de 9,94). Levar o par
 * {@code (soma, quantidade)} mantém o mesmo peso para todas as leituras, e a divisão acontece uma
 * única vez, no reducer.
 *
 * <p>O Hadoop serializa chaves e valores entre as fases de map e reduce, por isso a classe
 * implementa {@link Writable}: {@link #write} e {@link #readFields} precisam tratar os campos na
 * mesma ordem.
 */
public class SumCountWritable implements Writable {

    private double sum;
    private long count;

    /** O Hadoop cria os valores por reflexão e depois chama {@link #readFields}, então o construtor sem argumentos é obrigatório. */
    public SumCountWritable() {
    }

    public SumCountWritable(double sum, long count) {
        set(sum, count);
    }

    /** Permite ao mapper reutilizar uma única instância em vez de criar um objeto por linha. */
    public void set(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readLong();
    }

    /** Usado quando o valor é gravado em um arquivo de saída de texto. */
    @Override
    public String toString() {
        return sum + "\t" + count;
    }
}
