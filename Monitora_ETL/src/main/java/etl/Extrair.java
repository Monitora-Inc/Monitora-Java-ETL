package etl;

import java.io.*;
import java.util.*;

public class Extrair {

    public List<String[]> extrairDadosCSV(String csv) {
        List<String[]> dados = new ArrayList<>();

        try (BufferedReader leitor = new BufferedReader(new FileReader(csv))) {
            String line;
            while ((line = leitor.readLine()) != null) {
                String[] arrayLinha = line.split(";");
                dados.add(arrayLinha);
            }
        } catch (IOException e) {
            System.out.println("Erro ao ler o arquivo. " + e);
        }
        return dados;
    }
}





