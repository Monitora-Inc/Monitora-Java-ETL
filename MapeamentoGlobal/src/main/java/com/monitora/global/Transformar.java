package com.monitora.global;

import com.monitora.global.model.MetricaBruta;
import com.monitora.global.model.MetricaTratada;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Transformar {
    public MetricaTratada transformar(String idDataCenter, List<MetricaBruta> brutas, double limiteMb) {

        MetricaTratada mt = new MetricaTratada();

        mt.idDataCenter = idDataCenter;

        mt.timestamp = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        if (brutas == null || brutas.isEmpty()) {
            mt.cpuPercent = 0;
            mt.ramPercent = 0;
            mt.discoPercent = 0;
            mt.redeMb = 0;
            mt.saude = 0;
            mt.cor = "#FF3F72";
            mt.up = false;
            mt.diferenca = 0;
            return mt;
        }

        double somaCpu = 0;
        double somaRam = 0;
        double somaDisco = 0;
        double somaRede = 0;
        double somaLat = 0;

        int total = brutas.size();

        for (MetricaBruta m : brutas) {
            somaCpu += m.cpu;
            somaRam += m.ramPercent;
            somaDisco += m.discoPercent;
            somaRede += m.usoRedeMb;
            somaLat += m.latencia;
        }

        mt.cpuPercent = somaCpu / total;
        mt.ramPercent = somaRam / total;
        mt.discoPercent = somaDisco / total;
        mt.redeMb = somaRede / total;
        mt.redeMbS = (mt.redeMb / 3) / limiteMb*100;

        double latMed = somaLat / total;

        mt.saude = 100;
        mt.saude -= mt.cpuPercent * 0.15;
        mt.saude -= mt.ramPercent * 0.25;
        mt.saude -= mt.discoPercent * 0.20;
        mt.saude -= (latMed / 1000) * 0.4;

        if (mt.saude >= 90) mt.cor = "#00D4C4";
        else if (mt.saude >= 70) mt.cor = "#FFB94A";
        else mt.cor = "#FF3F72";

        mt.up = mt.saude >= 70;
        mt.diferenca = 0;
        return mt;
    }
}
