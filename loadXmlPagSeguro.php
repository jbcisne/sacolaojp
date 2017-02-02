<?php


class ImportaXml
{

    private $_xmlIterator = null;

    private $_db = null;

    private $_strInsertStatus = "INSERT INTO tb_transacao_status (cd_transacao, cd_status, dt_processamento) values (%s, %d, %s)";

    private $_arrConvert = [
        'Cliente_Nome'=> ['Venda pela Moderninha' => 1, 'juliano buzanello' => 2],
        'Debito_Credito' => ['Débito' => 1, 'Crédito' => 2],
        'Tipo_Transacao' => ['Saque' => 1, 'Pagamento' => 2 ],
        'Tipo_Pagamento' => ['Cartão de Débito' => 1, 'Cartão de Crédito' => 2],
        'Status' => ['Aprovada' => 1,
            'Aguardando pagamento' => 2,
            'Pagamento negado pela empresa de cartão de crédito' => 3],
    ];

    private $_arrKeyMap = array(
        'Transacao_ID' => array('type'=>'S', 'column' => 'cd_transacao'),
        'Cliente_Nome' => array('type'=>'I', 'column' => 'cd_tipo_cliente'),
        'Cliente_Email' => array('type'=>'S', 'column' => 'tx_cliente_email'),
        'Debito_Credito' => array('type'=>'I', 'column' => 'cd_metodo_transacao'),
        'Tipo_Transacao' => array('type'=>'I', 'column' => 'cd_tipo_transacao'),
        'Status' => array('type'=>'I', 'column' => 'cd_status'),
        'Tipo_Pagamento' => array('type'=>'I', 'column' => 'cd_tipo_pagamento'),
        'Valor_Bruto' => array('type'=>'F', 'column' => 'vl_bruto'),
        'Valor_Desconto' => array('type'=>'F', 'column' => 'vl_desconto'),
        'Valor_Taxa' => array('type'=>'F', 'column' => 'vl_taxa'),
        'Valor_Liquido' => array('type'=>'F', 'column' => 'vl_liquido'),
        'Data_Transacao' => array('type'=>'D', 'column' => 'dt_transacao'),
        'Data_Compensacao' => array('type'=>'D', 'column' => 'dt_compensacao'),
        'Parcelas' => array('type'=>'I', 'column' => 'nu_parcelas'),
        'Codigo_Usuario' => array('type'=>'I', 'column' => 'cd_usuario'),
        'Codigo_Venda' => array('type'=>'I', 'column' => 'cd_venda'),
        'Serial_Leitor' => array('type'=>'I', 'column' => 'nu_serial_leitor'),
        'Recebimentos' => array('type'=>'I', 'column' => 'nu_recebimentos'),
        'Recebidos' => array('type'=>'I', 'column' => 'nu_recebidos'),
        'Valor_Recebido' => array('type'=>'F', 'column' => 'vl_recebido'),
        'Valor_Tarifa_Intermediacao' => array('type'=>'F', 'column' => 'vl_tarifa_intermediacao'),
        'Valor_Taxa_Intermediacao' => array('type'=>'F', 'column' => 'vl_taxa_intermediacao'),
        'Valor_Taxa_Parcelamento' => array('type'=>'F', 'column' => 'vl_taxa_parcelamento'),
        'Valor_Tarifa_Boleto' => array('type'=>'F', 'column' => 'vl_tarifa_boleto'),
        );

    private $_arrKeys = [];
    private $_arrValues = [];

    public function __construct($file)
    {
        if (!file_exists($file)) {
            throw new \Exception('Xml file not found.');
        }
        $this->_xmlIterator = new SimpleXmlIterator($file, null, true);
        $this->_db = new Database();

    }

    public function process()
    {
        try{
            $this->_db->beginTransaction();

            for( $this->_xmlIterator->rewind(); $this->_xmlIterator->valid(); $this->_xmlIterator->next() ) {

                $this->_arrKeys = [];
                $this->_arrValues = [];

                $node = $this->_xmlIterator->getChildren();
                if (!$this->_hasTransacao($node)) {
                    $this->_processTransacaoInsert($node);
                }else{
                    $this->_processTransacaoUpdate($node);
                }
            }

            $this->_db->commit();
        }catch(\Exception $e) {
            $this->_db->rollback();
            echo '<pre>';
            var_dump($e->getMessage());
            die;
        }

        return $this;
    }

    private function _hasTransacao ($node)
    {
        $key = 'Transacao_ID';
        $childrenValue = $node->$key->__toString();
        $dbColumn = $this->_arrKeyMap[$key]['column'];

        $statement = $this->_db->prepare("SELECT $dbColumn FROM tb_transacao WHERE $dbColumn = '{$childrenValue}'");
        $statement->execute();

        $result = $statement->fetch(PDO::FETCH_ASSOC);

        if (!$result) {
            return false;
        }

        return true;
    }

    private function _processTransacaoInsert ($node)
    {
        foreach($node as $name => $data) {
            if (array_key_exists($name, $this->_arrConvert)) {
                $key = $data->__toString();
                if (array_key_exists($key,$this->_arrConvert[$name])) {
                    $data = $this->_arrConvert[$name][$key];
                    $this->_processValue($name,$data);
                }else{
                    throw new \Exception("Chave '$key' nao localizada na conversao do '$name'");
                }
            }else{
                if ($data instanceof SimpleXMLIterator ){
                    $data = $data->__toString();
                }
                $this->_processValue($name, $data);
            }
        }

        $strInsertTransacao       = "insert into tb_transacao (%s) values (%s)";
        $strInsertTransacaoStatus = $this->_strInsertStatus;
        $strInsertTransacaoStatus = sprintf($strInsertTransacaoStatus, $this->_arrValues['cd_transacao'], $this->_arrValues['cd_status'], "'" . date('Y-m-d H:i:s') . "'");


        //remove as chaves de status pois ja foram usadas na sql do status
        unset($this->_arrValues['cd_status'], $this->_arrKeys['cd_status']);

        $strSql = sprintf($strInsertTransacao, implode(', ', $this->_arrKeys), implode(', ', $this->_arrValues));

        $stantmentTransacao = $this->_db->prepare($strSql);
        $stantmentTransacaoStatus = $this->_db->prepare($strInsertTransacaoStatus);

        $stantmentTransacao->execute();
        $stantmentTransacaoStatus->execute();

        return $this;
    }

    private function _processTransacaoUpdate ($node)
    {
        $keyStatus = 'Status';
        $keyTransacao = 'Transacao_ID';
        $valueStatus = $node->$keyStatus->__toString();

        $strUpdateTransacao = "UPDATE tb_transacao SET %s WHERE cd_transacao = '%s'";

        $cdTransacao = $node->$keyTransacao->__toString();

        foreach($node as $name => $data) {
            if (array_key_exists($name, $this->_arrConvert)) {
                $key = $data->__toString();
                if (array_key_exists($key, $this->_arrConvert[$name])) {
                    $data = $this->_arrConvert[$name][$key];

                    if ($name == $keyStatus) {
                        $cdStatus = $this->_arrConvert[$keyStatus][$valueStatus];
                        continue;
                    }

                    $this->_processValue($name, $data);
                } else {
                    throw new \Exception("Chave '$key' nao localizada na conversao do '$name'");
                }
            } else {
                if ($data instanceof SimpleXMLIterator) {
                    $data = $data->__toString();
                }
                if ($name == $keyTransacao) {
                    continue;
                }
                $this->_processValue($name, $data);
            }
        }

        $aux = [];
        foreach ($this->_arrValues as $column=>$value) {
            $aux[] = $column . ' = '. $value;
        }

        $strInsertTransacaoStatus = $this->_strInsertStatus;
        $strInsertTransacaoStatus = sprintf($strInsertTransacaoStatus, "'{$node->Transacao_ID->__toString()}'", $cdStatus, "'" . date('Y-m-d H:i:s') . "'");
        $strUpdateTransacao       = sprintf($strUpdateTransacao, implode(', ', $aux), $cdTransacao);

        $this->_db->query($strInsertTransacaoStatus);
        $this->_db->query($strUpdateTransacao);

        return $this;
    }

    private function _dateConvert($str)
    {
        $dateTime = DateTime::createFromFormat('d/m/Y H:i:s', $str);
        return $dateTime->format('Y-m-d H:i:s');
    }

    private function _processValue ($key, $value)
    {
        if($value){

            switch ($this->_arrKeyMap[$key]['type']) {
                case 'S':
                    $value = "'$value'";
                    break;
                case 'I':
                    $value = (integer)$value;
                    break;
                case 'F':
                    $value = str_replace(',', '.', $value);
                    $value = ($value == '0.0000')?'null':$value;
                    break;
                case 'D':
                    $value = "'{$this->_dateConvert($value)}'";
                    break;
                default:
                    throw new \Exception('Ops nenhum tipo defido para insersao');
                    break;
            }
            $dbColumn = $this->_arrKeyMap[$key]['column'];
            $this->_arrKeys[$dbColumn] = $dbColumn;
            $this->_arrValues[$dbColumn] = $value;
        }
    }

}




class Database extends PDO{

    public function __construct(){
        parent::__construct("sqlite:" . realpath("db.sqlite"));

//        $this->exec("PRAGMA foreign_keys = ON;"); // enable foreign keys
    }
}


//$fname = 'PagSeguro_2017-01-16_14-15-40.xml';
$fname = 'PagSeguro_2017-02-01_12-29-56.xml';

$importarXml = (new ImportaXml($fname))->process();

