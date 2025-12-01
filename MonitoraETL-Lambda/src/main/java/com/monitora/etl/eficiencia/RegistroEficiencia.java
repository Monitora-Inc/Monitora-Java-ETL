package com.monitora.etl.eficiencia;

public class RegistroEficiencia {

    public String empresaId;
    public String uuidServidor;

    public String data;      // yyyy-MM-dd
    public String hora;      // HH
    public String minuto;    // mm

    public int minutosTotais;
    public int minutosFuncionando;

    public double cacheHit;
    public double cacheMiss;
    public double totalRequisicoes;

    public double eficiencia;     // %
    public double latMedia;       // ms

    public double p50;
    public double p90;
    public double p99;

    public double latenciaNota;   // 0 a 100
    public double disponibilidade;
    public double saude;
}
