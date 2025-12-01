package com.monitora.etl.eficiencia;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ExtrairEficiencia {

    private String extrairEmpresaIdDoPath(String key) {
        String[] p = key.split("/");
        return (p.length > 0) ? p[0] : "0";
    }

    private String extrairServidorIdDoPath(String key) {
        String[] p = key.split("/");
        return (p.length > 1) ? p[1] : "desconhecido";
    }

    public RegistroEficiencia processar(InputStream is, String key) throws Exception {

        RegistroEficiencia r = new RegistroEficiencia();

        // 1. empresa e servidor pelo path
        r.empresaId   = extrairEmpresaIdDoPath(key);
        r.uuidServidor = extrairServidorIdDoPath(key);

        // 2. lê CSV bruto
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String linha = br.readLine(); // header

        while ((linha = br.readLine()) != null) {
            String[] c = linha.split(";");

            // Ajusta índices de acordo com o teu CSV
            r.data   = c[0];  // yyyy-MM-dd
            r.hora   = c[1];  // HH
            r.minuto = c[2];  // mm

            r.cacheHit  = parseIntFlex(c[3]);
            r.cacheMiss = parseIntFlex(c[4]);
            r.latMedia  = Double.parseDouble(c[5]);

            r.totalRequisicoes = r.cacheHit + r.cacheMiss;
        }

        br.close();

        // minutos total/dia (por enquanto fixo)
        r.minutosTotais       = 1440;
        r.minutosFuncionando  = 1440;

        return r;
    }

    // Tenta int normal; se vier "7.970516992E9", converte como double e arredonda
    private int parseIntFlex(String s) {
        if (s == null || s.isBlank()) return 0;

        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            double d = Double.parseDouble(s);
            return (int) Math.round(d);
        }
    }
}
