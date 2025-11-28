package com.monitora.dc;
import com.monitora.dc.model.MetricaBruta;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Extrair {
    public List<MetricaBruta> extrair(InputStream csv) throws Exception {
        List<MetricaBruta> linhas = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(csv))) {
            String linha = reader.readLine();
            if (linha == null) {
                throw new RuntimeException("CSV vazio ou inválido.");
            }
            while ((linha = reader.readLine()) != null) {
                if (linha.trim().isEmpty()) continue;
                String[] col = linha.split(";");
                if (col.length != 23) {
                    throw new RuntimeException("Esperado 23 colunas, veio: " + col.length);
                }
                MetricaBruta mb = new MetricaBruta(col);
                linhas.add(mb);
            }
        }
        return linhas;
    }
}