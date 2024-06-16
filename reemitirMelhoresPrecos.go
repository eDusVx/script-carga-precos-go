package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type Preco struct {
	Valor          float64 `json:"valor"`
	FormaPagamento int     `json:"formaPagamento"`
	TipoPreco      int     `json:"tipoPreco"`
}

type Campanha struct {
	Nome           string `json:"nome"`
	InicioVigencia string `json:"inicioVigencia"`
	FimVigencia    string `json:"fimVigencia"`
}

type MelhorPreco struct {
	Preco    Preco    `json:"preco"`
	Campanha Campanha `json:"campanha"`
}

type Filial struct {
	IDFilial       int           `json:"idFilial"`
	MelhoresPrecos []MelhorPreco `json:"melhoresPrecos,omitempty"`
	PrecoLivro     *string       `json:"precoLivro,omitempty"`
}

type Produto struct {
	ID      int      `json:"id"`
	Filiais []Filial `json:"filiais"`
}

func main() {
	log.Println("INICIANDO SCRIPT")
	numeroEventos := 0
	numeroProdutos := 0
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Erro ao carregar o arquivo .env")
	}
	dbUser := os.Getenv("DB_USERNAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_DATABASE")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable", dbUser, dbPassword, dbName, dbHost, dbPort)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	produtosPorPagina := 1500
	var quantidadeProdutosBase int
	err = db.QueryRow("SELECT count(*) FROM produto").Scan(&quantidadeProdutosBase)
	if err != nil {
		log.Fatal(err)
	}
	paginaFinal := int(math.Ceil(float64(quantidadeProdutosBase) / float64(produtosPorPagina)))
	for pagina := 0; pagina < paginaFinal; pagina++ {
		log.Println("PÁGINA NÚMERO: ", pagina)
		log.Println("Página final: ", paginaFinal)
		offset := pagina * produtosPorPagina
		rows, err := db.Query("SELECT id, json_filial FROM produto ORDER BY id ASC LIMIT $1 OFFSET $2", produtosPorPagina, offset)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			start := time.Now()
			var produto Produto
			var jsonFilial string

			err := rows.Scan(&produto.ID, &jsonFilial)
			if err != nil {
				log.Fatal(err)
			}

			err = json.Unmarshal([]byte(jsonFilial), &produto.Filiais)
			if err != nil {
				log.Fatal(err)
			}

			log.Println("INICIANDO O PRODUTO DE ID: ", produto.ID)
			for _, filial := range produto.Filiais {
				var campanhas []map[string]interface{}

				for _, melhorPreco := range filial.MelhoresPrecos {
					campanha := map[string]interface{}{
						"preco": melhorPreco.Preco,
						"campanha": map[string]interface{}{
							"nome":           melhorPreco.Campanha.Nome,
							"inicioVigencia": melhorPreco.Campanha.InicioVigencia,
							"fimVigencia":    melhorPreco.Campanha.FimVigencia,
						},
					}
					campanhas = append(campanhas, campanha)
				}

				output := map[string]interface{}{
					"filial":     filial.IDFilial,
					"produto":    produto.ID,
					"campanhas":  campanhas,
					"precoLivro": filial.PrecoLivro,
				}

				jsonOutput, err := json.Marshal(output)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(string(jsonOutput))
				numeroEventos++
			}
			numeroProdutos++
			duration := time.Since(start)
			log.Printf("Tempo total de execução : %s para o produto: ", duration)
		}

		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
		log.Println("NUMERO DE EVENTOS", numeroEventos)

		log.Println("NUMEROPRODUTOS", numeroProdutos)
	}
	log.Println("Todos os produtos foram enviados para o Kafka")
}
