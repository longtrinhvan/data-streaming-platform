import React from "react";
import "../styles/Header.css";

const Header = () => {
  return (
    <header className="header">
      <h1>Blog</h1>
      <nav className="menu">
        <ul>
          <li className="dropdown">
            <a href="#menu" className="menu-icon">☰</a>
            <ul className="dropdown-content">
              <li><a href="#about">About</a></li>
              <li><a href="#projects">Projects</a></li>
              <li><a href="#contact">Contact</a></li>
            </ul>
          </li>
        </ul>
      </nav>
    </header>
  );
};

export default Header;
