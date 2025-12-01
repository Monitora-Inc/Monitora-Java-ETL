package com.monitora.etl.eficiencia;

public class TransformarEficiencia {

    public void transformar(RegistroEficiencia r) {

        // disponibilidade
        r.disponibilidade = (r.minutosFuncionando / (double) r.minutosTotais) * 100;

        // eficiência = cache_hit / total
        r.eficiencia = (r.cacheHit + r.cacheMiss == 0)
                ? 0
                : (r.cacheHit / (r.cacheHit + r.cacheMiss)) * 100;

        // latência média → nota de 0 a 100
        r.latenciaNota = Math.max(0, 100 - r.latMedia);

        // simular percentis
        r.p50 = r.latMedia * 0.8;
        r.p90 = r.latMedia * 1.2;
        r.p99 = r.latMedia * 1.5;

        // SAÚDE = 50% disp + 30% eficiência + 20% latênciaNota
        r.saude =
                (r.disponibilidade * 0.5) +
                        (r.eficiencia * 0.3) +
                        (r.latenciaNota * 0.2);

        // nunca permitir negativo
        r.saude = Math.max(0, r.saude);
    }
}
