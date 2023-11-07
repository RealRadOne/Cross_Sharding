use rand::distributions::{Distribution, Bernoulli, Uniform};
use rand_distr::Zipf;
use bytes::BufMut as _;
use bytes::BytesMut;
use rand::Rng;
use std::cmp;
use std::collections::HashSet;
use bytes::Bytes;

const MAX_DEPOSIT: u32 = 50;
const MAX_AMOUNT: u32 = u32::MAX;
const SPLIT_PARTY_SIZE_MIN: u32 = 3;
const SPLIT_PARTY_SIZE_MAX: u32 = 5;

struct SmallBank{
    // n_users: u64,
    checking_accounts: Vec<u32>,
    saving_accounts: Vec<u32>,
}

impl SmallBank{
    pub fn new(n_users: u64) -> Self {
        let mut checking_accounts: Vec<u32> = Vec::new();
        let mut saving_accounts: Vec<u32> = Vec::new();

        for _ in 0..n_users{
            checking_accounts.push(1000);
            saving_accounts.push(1000);
        }

        SmallBank {
            // n_users: n_users,
            checking_accounts: checking_accounts,
            saving_accounts: saving_accounts,
        }
    }

    // pub fn get_n_users(&self) -> u64{
    //     return self.n_users;
    // }

    pub fn deposit_checking(&mut self, user_id: u32, amount: u32){
        if MAX_AMOUNT - self.checking_accounts[user_id  as usize] > amount{
            self.checking_accounts[user_id  as usize] += amount;
        }
    }

    pub fn deposit_saving(&mut self, user_id: u32, amount: u32){
        if MAX_AMOUNT - self.saving_accounts[user_id  as usize] > amount{
            self.saving_accounts[user_id  as usize] += amount;
        }
    }

    pub fn withdraw_checking(&mut self, user_id: u32, amount: u32){
        if self.checking_accounts[user_id  as usize] >= amount{
            self.checking_accounts[user_id  as usize] -= amount;
        }
    }

    pub fn withdraw_saving(&mut self, user_id: u32, amount: u32){
        if self.saving_accounts[user_id  as usize] >= amount{
            self.saving_accounts[user_id  as usize] -= amount;
        }
    }

    pub fn get_checking_amount(&self, user_id: u32) -> u32{
        return self.checking_accounts[user_id as usize];
    }

    pub fn get_saving_amount(&self, user_id:u32) -> u32{
        return self.saving_accounts[user_id as usize];
    }
}

pub struct SmallBankTransactionHandler{
    tx_size: usize,
    // n_users: u64,
    // skew_factor: f64,
    // prob_choose_mtx: f64,
    user_distribution: Zipf<f64>,
    tx_type_distribution: Bernoulli,
    mtx_distribution: Uniform<i32>,
    small_bank: SmallBank,
}

impl SmallBankTransactionHandler{

    pub fn new(tx_size: usize, n_users: u64, skew_factor: f64, prob_choose_mtx: f64) -> Self {
        let user_distribution = Zipf::new(n_users-1, skew_factor).unwrap(); 
        let tx_type_distribution = Bernoulli::new(prob_choose_mtx).unwrap();
        let mtx_distribution = Uniform::from(0..6);
        let small_bank = SmallBank::new(n_users);

        SmallBankTransactionHandler {
            tx_size: tx_size,
            // n_users: n_users,
            // prob_choose_mtx: prob_choose_mtx,
            // skew_factor: skew_factor,
            user_distribution: user_distribution,
            tx_type_distribution: tx_type_distribution,
            mtx_distribution: mtx_distribution,
            small_bank: small_bank,
        }
    }

    fn _get_bytes_to_u32(&self, tx_bytes:&[u8]) -> u32{
        return u32::from_be_bytes(tx_bytes.try_into().unwrap());
    }

    fn _generate_random(&self, min:u32, max: u32) -> u32{
        let mut max = max;
        if max == MAX_AMOUNT{
            max = MAX_AMOUNT-1;
        }
        return rand::thread_rng().gen_range(min..max+1);
        // return rand::thread_rng().gen::<u32>() % max_number;
    }

    fn _sample_user(&self) -> u64{
        return self.user_distribution.sample(&mut rand::thread_rng()) as u64;
    }

    fn _sample_tx_type(&self) -> bool{
        return self.tx_type_distribution.sample(&mut rand::thread_rng());
    }

    fn _sample_mtx(&self) -> i32{
        return self.mtx_distribution.sample(&mut rand::thread_rng());
    }

    /// Transaction type: 0
    fn _generate_tx_deposit_saving(&self, tx: &mut BytesMut){
        // transaction_savings
        let user_id:u32 = self._sample_user() as u32;
        let amount_to_deposit: u32 = self._generate_random(
                                        0, 
                                        MAX_AMOUNT-self.small_bank.get_saving_amount(user_id)
                                    );

        // Update tx
        tx.put_u32(user_id);
        tx.put_u32(amount_to_deposit);
    }

    fn _execute_tx_deposit_saving(&mut self, tx: Bytes){
        let user_id: u32 = self._get_bytes_to_u32(&tx[2..6]);
        let amount_to_deposit: u32 = self._get_bytes_to_u32(&tx[6..10]);
        self.small_bank.deposit_saving(user_id, amount_to_deposit);
    }

    /// Transaction type: 1
    fn _generate_tx_deposit_checking(&self, tx: &mut BytesMut){
        // Deposit checking
        let user_id:u32 = self._sample_user() as u32;
        let amount_to_deposit: u32 = self._generate_random(
                                        0, 
                                        MAX_AMOUNT - self.small_bank.get_checking_amount(user_id)
                                    );

        // Update tx
        tx.put_u32(user_id);
        tx.put_u32(amount_to_deposit);
    }

    fn _execute_tx_deposit_checking(&mut self, tx: Bytes){
        let user_id: u32 = self._get_bytes_to_u32(&tx[2..6]);
        let amount_to_deposit: u32 = self._get_bytes_to_u32(&tx[6..10]);
        self.small_bank.deposit_checking(user_id, amount_to_deposit);
    }

    /// Transaction type: 2
    fn _generate_tx_write_cheque(&self, tx: &mut BytesMut){
        // write_cheque
        let user_id:u32 = self._sample_user() as u32;
        let amount_to_withdraw: u32 = self._generate_random(
                                        0, 
                                        self.small_bank.get_checking_amount(user_id)
                                    );

        // Update tx
        tx.put_u32(user_id);
        tx.put_u32(amount_to_withdraw);
    }

    fn _execute_tx_write_cheque(&mut self, tx: Bytes){
        let user_id: u32 = self._get_bytes_to_u32(&tx[2..6]);
        let amount_to_withdraw: u32 = self._get_bytes_to_u32(&tx[6..10]);
        self.small_bank.withdraw_checking(user_id, amount_to_withdraw);
    }

    /// Transaction type: 3
    fn _generate_tx_send(&self, tx: &mut BytesMut){
        // send_payment
        let from_user_id: u32 = self._sample_user() as u32;
        let to_user_id: u32 = self._sample_user() as u32;
        let amount_to_transfer: u32 = self._generate_random(
                                    0, 
                                    cmp::min(
                                        self.small_bank.get_checking_amount(from_user_id),
                                        MAX_AMOUNT - self.small_bank.get_checking_amount(to_user_id)
                                    )
                                );
        
        // Update tx
        tx.put_u32(from_user_id);
        tx.put_u32(to_user_id);
        tx.put_u32(amount_to_transfer);
    }

    fn _execute_tx_send(&mut self, tx: Bytes){
        let from_user_id: u32 = self._get_bytes_to_u32(&tx[2..6]);
        let to_user_id: u32 = self._get_bytes_to_u32(&tx[6..10]);
        let amount_to_transfer: u32 = self._get_bytes_to_u32(&tx[10..14]);
        self.small_bank.withdraw_checking(from_user_id, amount_to_transfer);
        self.small_bank.deposit_checking(to_user_id, amount_to_transfer);
    }

    /// Transaction type: 4
    fn _generate_tx_split(&self, tx: &mut BytesMut){
        // Split transaction
        let party_size: u32 = self._generate_random(
                                SPLIT_PARTY_SIZE_MIN,
                                SPLIT_PARTY_SIZE_MAX
                            );
        let n_payors: u32 = self._generate_random(
                                    1,
                                    party_size/2
                                );
        let n_payees: u32 = party_size - n_payors;
        let mut user_set: HashSet<u32> = HashSet::new();
        while user_set.len() < party_size as usize{
            user_set.insert(self._sample_user() as u32);
        }
        let mut amount_to_split: u32 = 0;
        let mut count: u32 = 0;
        // update transaction
        tx.put_u32(n_payors);
        tx.put_u32(n_payees);
        for user_id in user_set{
            tx.put_u32(user_id);
            if count<n_payors{
                let amount = self._generate_random(0,
                                cmp::min(MAX_DEPOSIT,
                                    self.small_bank.get_checking_amount(user_id)
                                )
                            );
                tx.put_u32(amount);
                amount_to_split += amount;
            }
            else{
                tx.put_u32(amount_to_split/n_payees);
                if count==party_size-1{
                    tx.put_u32(amount_to_split - ((n_payees-1)*(amount_to_split/n_payees)));
                }
            }
            count += 1;
        }
    }

    fn _execute_tx_split(&mut self, tx: Bytes){
        let n_payors: u32 = self._get_bytes_to_u32(&tx[2..6]);
        let n_payees: u32 = self._get_bytes_to_u32(&tx[6..10]);
        let mut start_byte = 10;

        for _ in 0..n_payors{
            let user_id: u32 = self._get_bytes_to_u32(&tx[start_byte..start_byte+4]);
            start_byte += 4;
            let amount: u32 = self._get_bytes_to_u32(&tx[start_byte..start_byte+4]);
            start_byte += 4;

            self.small_bank.withdraw_checking(user_id, amount);
        }

        for _ in 0..n_payees{
            let user_id: u32 = self._get_bytes_to_u32(&tx[start_byte..start_byte+4]);
            start_byte += 4;
            let amount: u32 = self._get_bytes_to_u32(&tx[start_byte..start_byte+4]);
            start_byte += 4;

            self.small_bank.deposit_checking(user_id, amount);
        }
    }

    /// Transaction type: 5
    fn _generate_tx_amalgamate(&self, tx: &mut BytesMut){
        // amalgamate
        let user_id: u32 = self._sample_user() as u32;
        // Update tx
        tx.put_u32(user_id);
    }

    fn _execute_tx_amalgamate(&mut self, tx: Bytes){
        let user_id: u32 = self._get_bytes_to_u32(&tx[2..6]);
        let saving_amount = self.small_bank.get_saving_amount(user_id);

        if  saving_amount<= MAX_AMOUNT-self.small_bank.get_checking_amount(user_id){
            self.small_bank.withdraw_saving(user_id, saving_amount);
            self.small_bank.deposit_checking(user_id, saving_amount);
        }
    }

    /// Transaction type: 6
    fn _generate_tx_read(&self, tx: &mut BytesMut){
        // Read
        let user_id: u32 = self._sample_user() as u32;
        // Update tx
        tx.put_u32(user_id);
    }

    fn _execute_tx_read(&mut self, tx: Bytes){
        let user_id: u32 = self._get_bytes_to_u32(&tx[2..6]);
        let _ = self.small_bank.get_checking_amount(user_id);
    }

    fn _generate_transaction(&self, tx_id:u8, sample_tx:bool) -> Bytes{
        let mut tx = BytesMut::with_capacity(self.tx_size);

        // Update first byte with indicator
        if sample_tx{
            tx.put_u8(0u8); // Sample txs start with 0.
        }
        else{
            tx.put_u8(1u8); // Standard txs start with 1.
        }

        // Update second byte with transaction id
        tx.put_u8(tx_id);

        match tx_id{
            0 => self._generate_tx_deposit_saving(&mut tx),
            1 => self._generate_tx_deposit_checking(&mut tx),
            2 => self._generate_tx_write_cheque(&mut tx),
            3 => self._generate_tx_send(&mut tx),
            4 => self._generate_tx_split(&mut tx),
            5 => self._generate_tx_amalgamate(&mut tx),
            _ => self._generate_tx_read(&mut tx),
        }

        // Resize transactions
        tx.resize(self.tx_size, 0u8);
        let tx_bytes = tx.split().freeze();

        return tx_bytes;
    }

    fn _execute_transaction(&mut self, tx_id: u8, tx: Bytes){
        match tx_id{
            0 => self._execute_tx_deposit_saving(tx),
            1 => self._execute_tx_deposit_checking(tx),
            2 => self._execute_tx_write_cheque(tx),
            3 => self._execute_tx_send(tx),
            4 => self._execute_tx_split(tx),
            5 => self._execute_tx_amalgamate(tx),
            _ => self._execute_tx_read(tx),
        }
    }
 

    pub fn get_next_transaction(&self, sample_tx:bool) -> Bytes{

        // get transaction id
        let mut tx_id:u8 =  6;
        if self._sample_tx_type()==true{
            tx_id = self._sample_mtx() as u8;
        }

        // Generate transaction
        return self._generate_transaction(tx_id, sample_tx);

    }


    pub fn execute_transaction(&mut self, tx: Bytes){
        // get transaction id
        let tx_id: u8 = tx[1];

        // Execute transaction
        self._execute_transaction(tx_id, tx)
    }

}