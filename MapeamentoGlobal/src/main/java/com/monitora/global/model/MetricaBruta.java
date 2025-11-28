package com.monitora.global.model;

public class MetricaBruta {
    public String idServidor;
    public String timestamp;
    public double cpu;
    public double totalRam;
    public double ramUsada;
    public double ramPercent;
    public double ramQuente;
    public double ramFria;
    public double discoPercent;
    public long bytesEnviados;
    public long bytesRecebidos;
    public double usoRedeMb;
    public double latencia;
    public long pacotesEnviados;
    public long pacotesRecebidos;
    public long pacotesPerdidos;
    public int qtdProcessos;
    public long uptimeSegundos;
    public String statusCpu;
    public String statusRam;
    public String statusDisco;
    public String statusUsoRede;
    public String statusLatencia;

    public MetricaBruta() {}
}