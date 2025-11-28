package com.monitora.global;

import com.monitora.global.model.MetricaBruta;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Extrair {
    public List<MetricaBruta> extrair(InputStream input) throws Exception {

        List<MetricaBruta> lista = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(input))) {

            String header = br.readLine();

            if (header == null) return lista;

            String linha;

            while ((linha = br.readLine()) != null) {
                if (linha.isBlank()) continue;
                String[] col = linha.split(";");

                if (col.length < 23) continue;
                MetricaBruta mb = new MetricaBruta();

                mb.idServidor = col[0];
                mb.timestamp = col[1];
                mb.cpu = Double.parseDouble(col[2]);
                mb.totalRam = Double.parseDouble(col[3]);
                mb.ramUsada = Double.parseDouble(col[4]);
                mb.ramPercent = Double.parseDouble(col[5]);
                mb.ramQuente = Double.parseDouble(col[6]);
                mb.ramFria = Double.parseDouble(col[7]);
                mb.discoPercent = Double.parseDouble(col[8]);
                mb.bytesEnviados = Long.parseLong(col[9]);
                mb.bytesRecebidos = Long.parseLong(col[10]);
                mb.usoRedeMb = Double.parseDouble(col[11]);
                mb.latencia = Double.parseDouble(col[12]);
                mb.pacotesEnviados = Long.parseLong(col[13]);
                mb.pacotesRecebidos = Long.parseLong(col[14]);
                mb.pacotesPerdidos = Long.parseLong(col[15]);
                mb.qtdProcessos = Integer.parseInt(col[16]);
                mb.uptimeSegundos = Long.parseLong(col[17]);
                mb.statusCpu = col[18];
                mb.statusRam = col[19];
                mb.statusDisco = col[20];
                mb.statusUsoRede = col[21];
                mb.statusLatencia = col[22];
                lista.add(mb);
            }
        }

        return lista;
    }
}
