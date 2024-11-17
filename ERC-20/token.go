package erc20

import (
	"errors"
	"fmt"
)

type ERC20Token struct {
	name        string
	symbol      string
	decimals    uint8
	totalSupply uint64
	balances    map[string]uint64
	allowances  map[string]map[string]uint64
}

// NewERC20Token is a constructor for the ERC20Token struct.
func NewERC20Token(name string, symbol string, decimals uint8, totalSupply uint64) *ERC20Token {
	token := &ERC20Token{
		name:        name,
		symbol:      symbol,
		decimals:    decimals,
		totalSupply: totalSupply,
		balances:    make(map[string]uint64),
		allowances:  make(map[string]map[string]uint64),
	}
	// Initially assign all tokens to the creator
	token.balances["creator"] = token.totalSupply
	return token
}

// Name returns the name of the token.
func (t *ERC20Token) Name() string {
	return t.name
}

// Symbol returns the symbol of the token.
func (t *ERC20Token) Symbol() string {
	return t.symbol
}

// Decimals returns the number of decimals the token uses.
func (t *ERC20Token) Decimals() uint8 {
	return t.decimals
}

// TotalSupply returns the total supply of tokens.
func (t *ERC20Token) TotalSupply() uint64 {
	return t.totalSupply
}

// BalanceOf returns the balance of a specific account.
func (t *ERC20Token) BalanceOf(account string) uint64 {
	return t.balances[account]
}

// Transfer transfers tokens from sender to recipient.
func (t *ERC20Token) Transfer(sender, recipient string, amount uint64) error {
	if t.balances[sender] < amount {
		return errors.New("ERC20: transfer amount exceeds balance")
	}
	t.balances[sender] -= amount
	t.balances[recipient] += amount
	fmt.Printf("Transfer: %s -> %s : %d tokens\n", sender, recipient, amount)
	return nil
}

// TransferFrom allows a spender to transfer tokens on behalf of the owner.
func (t *ERC20Token) TransferFrom(owner, spender, recipient string, amount uint64) error {
	if t.balances[owner] < amount {
		return errors.New("ERC20: transfer amount exceeds balance")
	}
	if t.allowances[owner][spender] < amount {
		return errors.New("ERC20: transfer amount exceeds allowance")
	}
	t.balances[owner] -= amount
	t.balances[recipient] += amount
	t.allowances[owner][spender] -= amount
	fmt.Printf("TransferFrom: %s -> %s : %d tokens via %s\n", owner, recipient, amount, spender)
	return nil
}

// Approve sets the allowance of spender for the caller's tokens.
func (t *ERC20Token) Approve(owner, spender string, amount uint64) {
	if t.allowances[owner] == nil {
		t.allowances[owner] = make(map[string]uint64)
	}
	t.allowances[owner][spender] = amount
	fmt.Printf("Approval: %s allows %s to spend %d tokens\n", owner, spender, amount)
}

// Allowance returns the remaining number of tokens that spender is allowed to spend.
func (t *ERC20Token) Allowance(owner, spender string) uint64 {
	if allowances, ok := t.allowances[owner]; ok {
		return allowances[spender]
	}
	return 0
}
